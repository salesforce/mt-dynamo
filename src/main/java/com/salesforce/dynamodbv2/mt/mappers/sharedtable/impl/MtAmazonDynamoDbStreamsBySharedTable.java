package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBase;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class MtAmazonDynamoDbStreamsBySharedTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbBySharedTable> {

    private static final int MAX_LIMIT = 1000;

    private final Timer getAllRecordsTime;
    private final DistributionSummary getAllRecordsSize;
    private final Timer getRecordsTime;
    private final DistributionSummary getRecordsSize;
    private final DistributionSummary getRecordsLoadedCounter;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb      corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
                                                MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams, mtDynamoDb);
        final MeterRegistry meterRegistry = mtDynamoDb.getMeterRegistry();
        final String name = MtAmazonDynamoDbStreamsBySharedTable.class.getSimpleName();
        getRecordsTime = meterRegistry.timer(name + ".GetRecords.Time");
        getRecordsSize = meterRegistry.summary(name + ".GetRecords.Size");
        getRecordsLoadedCounter = meterRegistry.summary(name + ".GetRecords.Loaded.Size");
        getAllRecordsTime = meterRegistry.timer(name + ".GetAllRecords.Time");
        getAllRecordsSize = meterRegistry.summary(name + ".GetAllRecords.Size");
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return super.getShardIterator(request);
    }

    /**
     * To get all records, use key prefix to determine how to map records. Does not check whether streaming is enabled
     * for a given tenant table, since the consumers of this API (such as KCL) currently expect to see all records in
     * the physical stream. It could be argued that this somewhat breaks the abstraction, since the client now has to
     * check on its end whether the virtual table for a given record has streaming enabled. On the other hand, we are
     * already breaking the abstraction by allowing calling without context, so hopefully that's acceptable.
     */
    @Override
    protected MtGetRecordsResult getAllRecords(GetRecordsRequest request, StreamArn streamArn) {
        return getAllRecordsTime.record(() -> {
            final MtGetRecordsResult result = getMtRecords(request, getRecordMapper(streamArn.getTableName()));
            getAllRecordsSize.record(result.getRecordCount());
            return result;
        });
    }

    private Function<Record, Optional<MtRecord>> getRecordMapper(String physicalTableName) {
        // get function that extracts tenant context and table name from key prefix
        final Function<Map<String, AttributeValue>, MtContextAndTable> contextParser =
            mtDynamoDb.getContextParser(physicalTableName);
        return record -> {
            // when called to map a record, extract tenant context and table
            final MtContextAndTable mtContextAndTable = contextParser.apply(record.getDynamodb().getKeys());
            // then establish context to map physical to virtual record, i.e., remove index key prefixes
            return mtDynamoDb.getMtContext().withContext(mtContextAndTable.getContext(), () -> {
                Optional<TableMapping> tableMapping = mtDynamoDb.getTableMapping(mtContextAndTable.getTableName());
                return tableMapping.map(TableMapping::getRecordMapper)
                    .map(f -> f.apply(record));
            });
        };
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
    @Override
    protected MtGetRecordsResult getRecords(GetRecordsRequest request, MtStreamArn mtStreamArn) {
        return getRecordsTime.record(() -> {
            final int limit = request.getLimit() == null ? MAX_LIMIT : request.getLimit();
            checkArgument(limit > 0 && limit <= MAX_LIMIT);

            final RecordMapper recordMapper = mtDynamoDb.getTableMapping(mtStreamArn.getTenantTableName())
                .orElseThrow(() -> new ResourceNotFoundException(mtStreamArn.getTenantTableName() + " does not exist"))
                .getRecordMapper();
            final Predicate<Record> recordFilter = recordMapper.createFilter();

            final long timeLimit = mtDynamoDb.getGetRecordsTimeLimit();
            final long time = mtDynamoDb.getClock().millis();

            final MtGetRecordsResult result = new MtGetRecordsResult()
                .withRecords(new ArrayList<>(limit))
                .withNextShardIterator(request.getShardIterator())
                .withRecordCount(0);

            MtGetRecordsResult partialResult;
            do {
                partialResult = getRecords(result.getNextShardIterator(),limit - result.getRecords().size(),
                    recordFilter, recordMapper, null);
                updateResult(result, partialResult);
            } while (result.getRecords().size() < limit        // only continue if we need more tenant records,
                && partialResult.getRecordCount() == MAX_LIMIT // have not reached current end of the underlying stream,
                && result.getNextShardIterator() != null       // have not reached absolute end of underlying stream,
                && (mtDynamoDb.getClock().millis() - time) <= timeLimit // and have not exceeded the soft time limit
            );

            getRecordsSize.record(result.getRecords().size());
            getRecordsLoadedCounter.record(result.getRecordCount());

            return result;
        });
    }

    /**
     * Fetches more records using the next iterator in the result and adds them to the result records. Returns the
     * number of records that were loaded from the underlying stream, so that the caller can decide whether to continue.
     */
    private MtGetRecordsResult getRecords(String shardIterator,
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
            return withMtRecordProperties(new MtGetRecordsResult()
                    .withRecords(mtRecords)
                    .withNextShardIterator(result.getNextShardIterator()),
                records);
        } else {
            // If we did not consume all records, then the loaded segment contains more tenant records than what we can
            // return per the client-specified limit. In that case we cannot use the loaded result, since the next shard
            // iterator would skip the records that were not returned. Therefore, we retry the load request with the
            // number of consumed records as the call limit, so that we get at most as many tenant records as needed for
            // the client-specified limit.
            if (retrieveMax == null) {
                return getRecords(shardIterator, limit, recordFilter, recordMapper, consumed);
            }
            // if we recurse, we should always be able to consume all records
            throw new IllegalStateException("Failed to load mt records for iterator " + shardIterator);
        }
    }

    private static void updateResult(MtGetRecordsResult result, MtGetRecordsResult partialResult) {
        result.setNextShardIterator(partialResult.getNextShardIterator());
        if (partialResult.getRecordCount() > 0) {
            if (result.getFirstSequenceNumber() == null) { // initialize for first result with records
                result
                    .withFirstSequenceNumber(partialResult.getFirstSequenceNumber())
                    .withFirstApproximateCreationDateTime(partialResult.getFirstApproximateCreationDateTime());
            }
            result
                .withRecordCount(result.getRecordCount() + partialResult.getRecordCount())
                .withLastSequenceNumber(partialResult.getLastSequenceNumber())
                .withLastApproximateCreationDateTime(partialResult.getLastApproximateCreationDateTime());
            result.getRecords().addAll(partialResult.getRecords());
        }
    }

}
