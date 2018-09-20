package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
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
     * Encapsulates logic of prefixing shard iterator with (physical) table name.
     */
    private static class ShardIterator {

        private static final String DELIMITER = "/";

        static ShardIterator parse(String s) {
            int idx = s.indexOf(DELIMITER);
            checkArgument(idx > 0, "Invalid shard iterator");
            String tableName = s.substring(0, idx);
            String shardIterator = s.substring(idx + 1);
            return new ShardIterator(tableName, shardIterator);
        }

        private final String tableName;
        private final String iterator;

        ShardIterator(String tableName, String iterator) {
            this.tableName = tableName;
            this.iterator = iterator;
        }

        ShardIterator next(String iterator) {
            return new ShardIterator(tableName, iterator);
        }

        @Override
        public String toString() {
            return String.join(DELIMITER, tableName, iterator);
        }
    }

    protected final T mtDynamoDb;
    private final LoadingCache<String, String> streamArnToTableName;

    protected MtAmazonDynamoDbStreamsBase(AmazonDynamoDBStreams streams, T mtDynamoDb) {
        super(streams);
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
            .withLimit(1)); // we don't need shards, but limit value must be > 0
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
            "listStreams currently does not support calling with tenant context");
        checkArgument(listStreamsRequest.getTableName() == null,
            "listStreams currently does not support filtering by table name");

        // filter to mt tables
        ListStreamsResult result = super.listStreams(listStreamsRequest);

        result.setStreams(result.getStreams().stream()
            .filter(stream -> mtDynamoDb.isMtTable(stream.getTableName()))
            .collect(toList()));

        // cache result before returning
        result.getStreams().forEach(s -> streamArnToTableName.put(s.getStreamArn(), s.getTableName()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("listStreams #streams={}, lastEvaluatedStreamArn={}",
                result.getStreams().size(), result.getLastEvaluatedStreamArn());
        }
        return result;
    }

    /**
     * Prepend physical table name to shard iterator, so that we can reverse transform records.
     *
     * @param request Shard iterator request.
     * @return Mt shard iterator.
     */
    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getShardIterator request={}", request);
        }

        GetShardIteratorResult result = super.getShardIterator(request);

        // prepend table name
        String tableName = streamArnToTableName.getUnchecked(request.getStreamArn());
        ShardIterator shardIterator = new ShardIterator(tableName, result.getShardIterator());
        result.setShardIterator(shardIterator.toString());

        if (LOG.isDebugEnabled()) {
            LOG.debug("getShardIterator result={}", result);
        }
        return result;
    }

    /**
     * Returns records from the underlying stream for the given context.
     *
     * @param request Record request. Maybe with or without tenant context.
     * @return Records for current context for the given request.
     */
    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords request={}", request);
        }

        ShardIterator iterator = ShardIterator.parse(request.getShardIterator());

        GetRecordsResult result = getRecords(
            getMtRecordMapper(iterator.tableName),
            request.withShardIterator(iterator.iterator));

        Optional.ofNullable(result.getNextShardIterator())
            .map(iterator::next)
            .map(ShardIterator::toString)
            .ifPresent(result::setNextShardIterator);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords response=(#records={}, iterator={})",
                result.getRecords().size(), result.getNextShardIterator());
        }
        return result;
    }

    protected GetRecordsResult getRecords(Function<Record, MtRecord> recordMapper,
        GetRecordsRequest getRecordsRequest) {
        return mapResult(recordMapper, super.getRecords(getRecordsRequest));
    }

    protected GetRecordsResult mapResult(Function<Record, MtRecord> recordMapper, GetRecordsResult result) {
        return new GetRecordsResult()
            .withNextShardIterator(result.getNextShardIterator())
            .withRecords(result.getRecords().stream()
                .map(recordMapper)
                .filter(this::matchesContext)
                .collect(toList()));
    }

    private boolean matchesContext(MtRecord mtRecord) {
        return mtDynamoDb.getMtContext().getContextOpt().map(mtRecord.getContext()::equals).orElse(true);
    }

    protected abstract Function<Record, MtRecord> getMtRecordMapper(String tableName);

}
