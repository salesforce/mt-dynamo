package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MtAmazonDynamoDbStreamsBySharedTable extends DelegatingAmazonDynamoDbStreams implements
        MtAmazonDynamoDbStreams {

    private static final Logger LOG = LoggerFactory.getLogger(MtAmazonDynamoDbStreamsBySharedTable.class);
    private static final String DELIMITER = "/";
    private static final int MAX_LIMIT = 1000;

    private final MtAmazonDynamoDbBySharedTable mtDynamoDb;
    private final LoadingCache<String, String> streamArnToTableName;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb            corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
                                                MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams);
        this.mtDynamoDb = mtDynamoDb;
        this.streamArnToTableName = CacheBuilder.newBuilder().build(CacheLoader.from(this::getTableName));
    }

    /**
     * Looks up the table name for the given stream.
     *
     * @param streamArn Stream to look up
     * @return Corresponding table name.
     */
    private String getTableName(String streamArn) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getTableName streamArn={}", streamArn);
        }

        DescribeStreamResult result = super.describeStream(new DescribeStreamRequest()
                .withStreamArn(streamArn)
                .withLimit(0));
        String tableName = result.getStreamDescription().getTableName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("getTableName result={}", tableName);
        }
        return tableName;
    }

    /**
     * Returns streams associated with the corresponding MT shared table instance.
     *
     * @param listStreamsRequest Stream request. Currently doesn't support filtering by table.
     * @return Result.
     */
    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("listStreams request={}", listStreamsRequest);
        }

        checkArgument(!mtDynamoDb.getMtContext().getContextOpt().isPresent(),
                "listStreams for shared table currently does not support calling with tenant context");
        checkArgument(listStreamsRequest.getTableName() == null,
                "listStreams for shared table currently does not support filtering by table name");

        // filter to mt tables
        ListStreamsResult result = super.listStreams(listStreamsRequest);
        Set<String> sharedTables = mtDynamoDb.getSharedTables()
                .stream()
                .map(CreateTableRequest::getTableName)
                .collect(toSet());
        List<Stream> mtStreams = result.getStreams().stream()
                .filter(stream -> sharedTables.contains(stream.getTableName()))
                .collect(toList());

        // update cache
        mtStreams.forEach(s -> streamArnToTableName.put(s.getStreamArn(), s.getTableName()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("listStreams #streams={}, lastEvaluatedStreamArn={}",
                    mtStreams.size(), result.getLastEvaluatedStreamArn());
        }
        return result.withStreams(mtStreams);
    }

    /**
     * Prepend physical table name to shard iterator, so that we can reverse transform records.
     *
     * @param getShardIteratorRequest Shard iterator request.
     * @return Mt shard iterator.
     */
    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        GetShardIteratorResult result = super.getShardIterator(getShardIteratorRequest);

        // prepend table name
        String tableName = streamArnToTableName.getUnchecked(getShardIteratorRequest.getStreamArn());
        String shardIterator = String.join(DELIMITER, tableName, result.getShardIterator());

        return result.withShardIterator(shardIterator);
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        String mtShardIterator = request.getShardIterator();
        int idx = mtShardIterator.indexOf(DELIMITER);
        checkArgument(idx > 0, "Invalid shard iterator");
        String tableName = mtShardIterator.substring(0, idx);
        String shardIterator = mtShardIterator.substring(idx + 1);

        Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction =
                mtDynamoDb.getFieldValueFunction(tableName);
        request.setShardIterator(shardIterator);

        GetRecordsResult result = super.getRecords(request);
        GetRecordsResult mtResult = toMtResult(fieldValueFunction, result);

        // keep fetching records if we haven't reached the limit yet
        int limit = Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT);
        while (mtResult.getRecords().size() < limit
                && !result.getRecords().isEmpty()
                && result.getNextShardIterator() != null) {
            GetRecordsRequest nextRequest = new GetRecordsRequest()
                    .withShardIterator(result.getNextShardIterator())
                    .withLimit(limit);
            GetRecordsResult nextResult = super.getRecords(nextRequest);
            GetRecordsResult nextMtResult = toMtResult(fieldValueFunction, nextResult);
            int unionSize = mtResult.getRecords().size() + nextMtResult.getRecords().size();
            if (unionSize > limit) {
                break;
            }
            result = nextResult;
            List<Record> union = new ArrayList<>(unionSize);
            union.addAll(mtResult.getRecords());
            union.addAll(nextMtResult.getRecords());
            mtResult = nextMtResult.withRecords(union);
        }

        return mtResult;
    }

    private GetRecordsResult toMtResult(Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction,
                                        GetRecordsResult result) {
        Function<Record, MtRecord> toMtRecord = record -> toMtRecord(fieldValueFunction, record);
        return new GetRecordsResult()
                .withNextShardIterator(result.getNextShardIterator())
                .withRecords(result.getRecords().stream()
                        .map(toMtRecord)
                        .filter(this::matchesContext)
                        .collect(toList()));
    }

    private MtRecord toMtRecord(Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction,
                                Record record) {
        FieldValue fieldValue = fieldValueFunction.apply(record.getDynamodb().getKeys());
        MtAmazonDynamoDbContextProvider mtContext = mtDynamoDb.getMtContext();
        // execute in record tenant context to get table mapping
        TableMapping tableMapping = mtContext.withContext(fieldValue.getMtContext(),
                mtDynamoDb::getTableMapping, fieldValue.getTableIndex());
        ItemMapper itemMapper = tableMapping.getItemMapper();
        StreamRecord streamRecord = record.getDynamodb();
        return new MtRecord()
                .withAwsRegion(record.getAwsRegion())
                .withEventID(record.getEventID())
                .withEventName(record.getEventName())
                .withEventSource(record.getEventSource())
                .withEventVersion(record.getEventVersion())
                .withContext(fieldValue.getMtContext())
                .withTableName(fieldValue.getTableIndex())
                .withDynamodb(new StreamRecord()
                        .withKeys(itemMapper.reverse(streamRecord.getKeys()))
                        .withNewImage(itemMapper.reverse(streamRecord.getNewImage()))
                        .withOldImage(itemMapper.reverse(streamRecord.getOldImage()))
                        .withSequenceNumber(streamRecord.getSequenceNumber())
                        .withStreamViewType(streamRecord.getStreamViewType())
                        .withApproximateCreationDateTime(streamRecord.getApproximateCreationDateTime())
                        .withSizeBytes(streamRecord.getSizeBytes()));
    }

    private boolean matchesContext(MtRecord mtRecord) {
        return mtDynamoDb.getMtContext().getContextOpt().map(mtRecord.getContext()::equals).orElse(true);
    }

    @Override
    public AmazonDynamoDBStreams getAmazonDynamoDbStreams() {
        return delegate;
    }

}
