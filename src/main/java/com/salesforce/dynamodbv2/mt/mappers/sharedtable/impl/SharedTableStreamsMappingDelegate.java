package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBase.createStreamSegmentMetrics;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable.MAX_LIMIT;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams.MtGetRecordsResult;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams.StreamSegmentMetrics;
import com.salesforce.dynamodbv2.mt.util.ShardIterator;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Maps virtual table change event stream requests to underlying "physical" {@link AmazonDynamoDBStreams}
 * dynamoDbStreams instance.
 */
public class SharedTableStreamsMappingDelegate {

    private final AmazonDynamoDBStreams dynamoDbStreams;

    // limit max time to execute getRecords before returning _something_ to the caller
    private final Clock clock;
    private final long getRecordsTimeLimit;

    // meters
    private final Timer getRecordsTime;
    private final DistributionSummary getRecordsSize;
    private final DistributionSummary getRecordsLoadedCounter;

    public SharedTableStreamsMappingDelegate(AmazonDynamoDBStreams dynamoDbStreams,
                                             Clock clock,
                                             long getRecordsTimeLimit,
                                             MeterRegistry meterRegistry) {
        this.dynamoDbStreams = dynamoDbStreams;
        this.clock = clock;
        this.getRecordsTimeLimit = getRecordsTimeLimit;

        String name = getClass().getSimpleName(); // TODO this does not look right
        getRecordsTime = meterRegistry.timer(name + ".GetRecords.Time");
        getRecordsSize = meterRegistry.summary(name + ".GetRecords.Size");
        getRecordsLoadedCounter = meterRegistry.summary(name + ".GetRecords.Loaded.Size");
    }

    public GetShardIteratorResult getShardIterator(TableMapping mapping, GetShardIteratorRequest request) {
        MtStreamArn arn = checkArn(mapping, StreamArn.fromString(request.getStreamArn()));

        // map logical to physical request
        request = request.clone().withStreamArn(arn.toDynamoDbArn());

        // call DynamoDB
        GetShardIteratorResult result = dynamoDbStreams.getShardIterator(request);

        // map physical result back to logical
        ShardIterator iterator = ShardIterator.fromString(result.getShardIterator());
        checkArgument(arn.toDynamoDbArn().equals(iterator.getArn())); // sanity check
        result.setShardIterator(iterator.withArn(request.getStreamArn()).toString());

        return result;
    }

    private MtStreamArn checkArn(TableMapping tableMapping, StreamArn arn) {
        checkArgument(arn instanceof MtStreamArn, "unsupported stream arn", arn);
        checkArgument(tableMapping.getVirtualTable().getTableName().equals(arn.getTableName()),
            "unexpected stream table %s", arn.getTableName());
        return (MtStreamArn) arn;
    }

    public GetRecordsResult getRecords(TableMapping mapping, GetRecordsRequest request) {
        // parse shard iterator and stream arn
        final ShardIterator iterator = ShardIterator.fromString(request.getShardIterator());
        final String arn = iterator.getArn();
        final MtStreamArn streamArn = checkArn(mapping, StreamArn.fromString(iterator.getArn()));

        // transform tenant-aware into DynamoDB iterator request
        request = new GetRecordsRequest()
            .withShardIterator(iterator.withArn(streamArn.toDynamoDbArn()).toString())
            .withLimit(request.getLimit());

        final MtGetRecordsResult result = getMtRecords(mapping, request, streamArn);

        // translate back to tenant-aware iterator
        Optional.ofNullable(result.getNextShardIterator())
            .map(it -> ShardIterator.fromString(it).withArn(arn).toString())
            .ifPresent(result::setNextShardIterator);

        return result;
    }

    /**
     * To fetch records for a specific tenant, fetch records from underlying stream, filter to only those records that
     * match the requested tenant table, and then convert them. Keeps fetching records from stream until it reaches:
     * <ol>
     * <li>limit,</li>
     * <li>current or absolute end of shard, or</li>
     * <li>time bound</li>
     * </ol>
     * Returns a subtype of {@link GetRecordsResult} that includes the last consumed sequence number from the underlying
     * stream shard, so that clients can request a new shard iterator where they left off.
     * Note that the method does not check whether streaming is enabled for the given tenant table. That check is
     * already done when obtaining a tenant ARN; once we allow updating tables (e.g., turning off streaming) we may need
     * to check here as well.
     */
    public MtGetRecordsResult getMtRecords(TableMapping mapping, GetRecordsRequest request, MtStreamArn streamArn) {
        return getRecordsTime.record(() -> {
            final int limit = request.getLimit() == null ? MAX_LIMIT : request.getLimit();
            checkArgument(limit > 0 && limit <= MAX_LIMIT);

            final RecordMapper recordMapper = mapping.getRecordMapper();
            final Predicate<Record> recordFilter = recordMapper.createFilter();

            final long timeLimit = getRecordsTimeLimit;
            final long time = clock.millis();

            final MtGetRecordsResult result = new MtGetRecordsResult()
                .withRecords(new ArrayList<>(limit))
                .withNextShardIterator(request.getShardIterator());

            int loaded;
            do {
                // Load a chunk of records
                final MtGetRecordsResult partialResult = getMtRecords(
                    result.getNextShardIterator(),
                    limit - result.getRecords().size(),
                    recordFilter,
                    recordMapper,
                    null);
                // Update result with what we got
                result.getRecords().addAll(partialResult.getRecords());
                result.setNextShardIterator(partialResult.getNextShardIterator());
                final StreamSegmentMetrics metrics = result.getStreamSegmentMetrics();
                if (metrics == null) {
                    result.setStreamSegmentMetrics(partialResult.getStreamSegmentMetrics());
                } else {
                    final StreamSegmentMetrics partialMetrics = partialResult.getStreamSegmentMetrics();
                    metrics.setRecordCount(metrics.getRecordCount() + partialMetrics.getRecordCount());
                    metrics.setLastRecordMetrics(partialMetrics.getLastRecordMetrics());
                }
                // Keep track of how much we we loaded to determine whether to keep going
                // Note: this heuristic is only accurate if we hit the streams cache or if DynamoDB happens to return
                // the max number of records. If we miss the cache and DynamoDB reaches the max result size (1MB) before
                // hitting the 1k record limit, we will stop. Also, DynamoDB sometimes has gaps in the stream where it
                // returns fewer records regardless of the limit.
                // TODO add a builder extensions that allows clients to specify the shard end tracked externally
                loaded = partialResult.getStreamSegmentMetrics().getRecordCount();
            } while (result.getRecords().size() < limit     // only continue if we need more tenant records,
                && loaded == MAX_LIMIT                      // have not reached current end of the underlying stream,
                && result.getNextShardIterator() != null    // have not reached absolute end of underlying stream,
                && (clock.millis() - time) <= timeLimit // and have not exceeded the soft time limit
            );

            getRecordsSize.record(result.getRecords().size());
            getRecordsLoadedCounter.record(result.getStreamSegmentMetrics().getRecordCount());

            return result;
        });
    }

    /**
     * Fetches more records using the next iterator in the result and adds them to the result records. Returns the
     * number of records that were loaded from the underlying stream, so that the caller can decide whether to continue.
     */
    private MtGetRecordsResult getMtRecords(String shardIterator,
                                            int limit,
                                            Predicate<Record> recordFilter,
                                            Function<Record, MtRecord> recordMapper,
                                            Integer retrieveMax) {
        // retrieve records from underlying stream
        final GetRecordsRequest request = new GetRecordsRequest()
            .withShardIterator(shardIterator)
            .withLimit(retrieveMax == null ? MAX_LIMIT : retrieveMax);
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();

        // otherwise, transform records and add those that match the createFilter
        final List<Record> mtRecords = new ArrayList<>(limit);
        int consumed = 0;
        for (Record record : records) {
            if (recordFilter.test(record)) {
                if (mtRecords.size() >= limit) {
                    break;
                } else {
                    mtRecords.add(recordMapper.apply(record));
                }
            }
            consumed++;
        }

        // If we consumed all records, then we can return the records and next shard iterator
        if (consumed == records.size()) {
            return new MtGetRecordsResult()
                .withRecords(mtRecords)
                .withNextShardIterator(result.getNextShardIterator())
                .withStreamSegmentMetrics(createStreamSegmentMetrics(records));
        } else {
            // If we did not consume all records, then the loaded segment contains more tenant records than what we can
            // return per the client-specified limit. In that case we cannot use the loaded result, since the next shard
            // iterator would skip the records that were not returned. Therefore, we retry the load request with the
            // number of consumed records as the call limit, so that we get at most as many tenant records as needed for
            // the client-specified limit.
            if (retrieveMax == null) {
                return getMtRecords(shardIterator, limit, recordFilter, recordMapper, consumed);
            }
            // if we recurse, we should always be able to consume all records
            throw new IllegalStateException("Failed to load mt records for iterator " + shardIterator);
        }
    }

}
