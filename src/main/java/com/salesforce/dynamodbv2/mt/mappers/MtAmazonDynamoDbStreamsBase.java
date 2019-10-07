package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.util.ShardIterator;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.DistributionSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class facilitates constructing multitenant records by encoding (physical) table name in shard iterators. Also
 * filters streams and records by current multitenant instance and context.
 */
public abstract class MtAmazonDynamoDbStreamsBase<T extends MtAmazonDynamoDbBase> extends
    DelegatingAmazonDynamoDbStreams implements MtAmazonDynamoDbStreams {

    private static final Logger LOG = LoggerFactory.getLogger(MtAmazonDynamoDbStreamsBase.class);

    /**
     * Parses arn and validates that tenant context matches current thread tenant context.
     *
     * @param arn Arn to parse.
     * @return Parsed Arn.
     */
    private StreamArn parseAndValidateContext(String arn) {
        final StreamArn parsedArn = StreamArn.fromString(arn);
        checkArgument(parsedArn.getContextOpt().equals(mtDynamoDb.getMtContext().getContextOpt()),
            "Current context does not match ARN context");
        return parsedArn;
    }

    protected final T mtDynamoDb;

    protected MtAmazonDynamoDbStreamsBase(AmazonDynamoDBStreams streams, T mtDynamoDb) {
        super(streams);
        this.mtDynamoDb = mtDynamoDb;
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

        checkArgument(mtDynamoDb.getMtContext().getContextOpt().isEmpty(),
            "listStreams currently does not support calling any tenant context except the empty context");
        checkArgument(listStreamsRequest.getTableName() == null,
            "listStreams currently does not support filtering by table name");

        // filter to mt tables
        ListStreamsResult result = dynamoDbStreams.listStreams(listStreamsRequest);

        result.setStreams(result.getStreams().stream()
            .filter(stream -> mtDynamoDb.isMtTable(stream.getTableName()))
            .collect(toList()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("listStreams #streams={}, lastEvaluatedStreamArn={}",
                result.getStreams().size(), result.getLastEvaluatedStreamArn());
        }
        return result;
    }

    /**
     * Translates between virtual and physical stream ARNs.
     *
     * @param describeStreamRequest Describe stream request.
     * @return Result
     */
    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest)
        throws AmazonDynamoDBException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("describeStream request={}", describeStreamRequest);
        }

        String arn = describeStreamRequest.getStreamArn();
        StreamArn streamArn = parseAndValidateContext(arn);
        DescribeStreamRequest request = describeStreamRequest.clone().withStreamArn(streamArn.toDynamoDbArn());

        DescribeStreamResult result = dynamoDbStreams.describeStream(request);

        StreamDescription description = result.getStreamDescription();
        streamArn.getTenantTableNameOpt().ifPresent(description::setTableName);
        description.setStreamArn(arn);

        if (LOG.isDebugEnabled()) {
            LOG.debug("describeStream result={}", result);
        }
        return result;
    }

    /**
     * Translates between virtual and physical stream ARNs.
     *
     * @param getShardIteratorRequest Shard iterator request.
     * @return Mt shard iterator.
     */
    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getShardIterator request={}", getShardIteratorRequest);
        }

        String arn = getShardIteratorRequest.getStreamArn();
        String dynamoDbArn = parseAndValidateContext(arn).toDynamoDbArn();
        GetShardIteratorRequest request = getShardIteratorRequest.clone().withStreamArn(dynamoDbArn);

        GetShardIteratorResult result = dynamoDbStreams.getShardIterator(request);

        ShardIterator iterator = ShardIterator.fromString(result.getShardIterator());
        checkArgument(dynamoDbArn.equals(iterator.getArn()));
        result.setShardIterator(iterator.withArn(arn).toString());

        if (LOG.isDebugEnabled()) {
            LOG.debug("getShardIterator result={}", result);
        }
        return result;
    }

    /**
     * Returns records from the underlying stream for the given context.
     *
     * @param getRecordsRequest Record request. Maybe with or without tenant context.
     * @return Records for current context for the given request.
     */
    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords request={}", getRecordsRequest);
        }

        final ShardIterator iterator = ShardIterator.fromString(getRecordsRequest.getShardIterator());
        final String arn = iterator.getArn();
        final StreamArn streamArn = parseAndValidateContext(arn);

        // transform tenant-aware into DynamoDB iterator request
        final GetRecordsRequest request = new GetRecordsRequest()
            .withShardIterator(iterator.withArn(streamArn.toDynamoDbArn()).toString())
            .withLimit(getRecordsRequest.getLimit());

        final MtGetRecordsResult result = streamArn instanceof MtStreamArn
            ? getRecords(request, (MtStreamArn) streamArn)
            : getAllRecords(request, streamArn);

        // translate back to tenant-aware iterator
        Optional.ofNullable(result.getNextShardIterator())
            .map(it -> ShardIterator.fromString(it).withArn(arn).toString())
            .ifPresent(result::setNextShardIterator);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords response=(#records={}, iterator={})",
                result.getRecords().size(), result.getNextShardIterator());
        }
        return result;
    }

    /**
     * Get only records for the given tenant context and table.
     *
     * @param request     GetRecordsRequest for physical stream.
     * @param mtStreamArn MtStreamArn containing physical table and stream names as well as context and tenant-table
     *                    name.
     * @return MtGetRecordsResult containing converted records.
     */
    protected abstract MtGetRecordsResult getRecords(GetRecordsRequest request, MtStreamArn mtStreamArn);

    /**
     * Get all records present in the physical stream outside of the context of a given tenant.
     *
     * @param request   GetRecordsRequest for physical stream.
     * @param streamArn StreamArn containing the physical table and stream names.
     * @return MtGetRecordsResult containing converted records.
     */
    protected abstract MtGetRecordsResult getAllRecords(GetRecordsRequest request, StreamArn streamArn);

    /**
     * Shared helper method for subclasses.
     *
     * @param request GetRecordsRequest for physical stream.
     * @param mapper  Function for mapping Records to MtRecords.
     * @param meter   Meter to emit size of returned records collection.
     * @return MtGetRecordsResult result.
     */
    protected MtGetRecordsResult getMtRecords(GetRecordsRequest request,
                                              Function<Record, Optional<MtRecord>> mapper,
                                              DistributionSummary meter) {
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();
        final String nextIterator = result.getNextShardIterator();
        if (records.isEmpty()) {
            meter.record(0);
            return new MtGetRecordsResult().withRecords(records).withNextShardIterator(nextIterator);
        }
        final List<Record> mtRecords = new ArrayList<>(records.size());
        for (Record record : records) {
            Optional<MtRecord> mtRecord = mapper.apply(record);
            if (mtRecord.isPresent()) {
                mtRecords.add(mtRecord.get());
            }
        }
        meter.record(mtRecords.size());
        return new MtGetRecordsResult()
            .withRecords(mtRecords)
            .withNextShardIterator(nextIterator)
            .withLastSequenceNumber(getLast(mtRecords).getDynamodb().getSequenceNumber());
    }

}
