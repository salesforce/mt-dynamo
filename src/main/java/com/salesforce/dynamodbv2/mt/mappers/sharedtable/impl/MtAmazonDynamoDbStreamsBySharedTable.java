package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.util.concurrent.UncheckedExecutionException;
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
        return getAllRecordsTime.record(() ->
            getMtRecords(request, getRecordMapper(streamArn.getTableName()), getAllRecordsSize));
    }

    private Function<Record, MtRecord> getRecordMapper(String physicalTableName) {
        // get function that extracts tenant context and table name from key prefix
        final Function<Map<String, AttributeValue>, FieldValue<?>> fieldValueFunction =
            mtDynamoDb.getFieldValueFunction(physicalTableName);
        return record -> {
            // when called to map a record, extract tenant context and table
            final FieldValue<?> fieldValue = fieldValueFunction.apply(record.getDynamodb().getKeys());
            // then establish context to map physical to virtual record, i.e., remove index key prefixes
            return mtDynamoDb.getMtContext().withContext(fieldValue.getContext(), () -> {
                try {
                    return mtDynamoDb.getTableMapping(fieldValue.getTableName()).getRecordMapper().apply(record);

                } catch (UncheckedExecutionException e) {
                    // This isn't great, but we're assuming a missing virtual table entry here is a deleted virtual
                    // table, and thus, shouldn't be mapping records to MtRecords. Instead return null, but really,
                    // we should make sure we let deleted records flow through a stream and expire out, before removing
                    // the virtual table entry.
                    if (e.getCause() instanceof ResourceNotFoundException) {
                        return null;
                    }
                    throw e;
                } catch (ResourceNotFoundException e) {
                    return null;
                }
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
            Optional.ofNullable(request.getLimit()).ifPresent(limit -> checkArgument(limit > 0 && limit <= MAX_LIMIT));

            final int limit = Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT);
            final long timeLimit = mtDynamoDb.getGetRecordsTimeLimit();
            final long time = mtDynamoDb.getClock().millis();

            final MtGetRecordsResult result = new MtGetRecordsResult()
                .withRecords(new ArrayList<>(limit))
                .withNextShardIterator(request.getShardIterator());

            final RecordMapper recordMapper =
                mtDynamoDb.getTableMapping(mtStreamArn.getTenantTableName()).getRecordMapper();
            final Predicate<Record> recordFilter = recordMapper.createFilter();

            int recordsLoaded;
            int recordsLoadedSum = 0;
            do {
                recordsLoaded = loadRecords(result, limit, recordFilter, recordMapper);
                recordsLoadedSum += recordsLoaded;
            } while (result.getRecords().size() < limit     // only continue if we need more tenant records,
                && recordsLoaded == MAX_LIMIT               // have not reached current end of the underlying stream,
                && result.getNextShardIterator() != null    // have not reached absolute end of underlying stream,
                && (mtDynamoDb.getClock().millis() - time) <= timeLimit // and have not exceeded the soft time limit
            );

            getRecordsSize.record(result.getRecords().size());
            getRecordsLoadedCounter.record(recordsLoadedSum);

            return result;
        });

    }

    /**
     * Fetches more records using the next iterator in the result and adds them to the result records. Returns the
     * number of records that were loaded from the underlying stream, so that the caller can decide whether to continue.
     */
    private int loadRecords(MtGetRecordsResult mtResult,
                            int limit,
                            Predicate<Record> recordFilter,
                            Function<Record, MtRecord> recordMapper) {
        // retrieve max number of records from underlying stream (and retry below if we got too many)
        final GetRecordsRequest request = new GetRecordsRequest().withLimit(MAX_LIMIT)
            .withShardIterator(mtResult.getNextShardIterator());
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();

        // otherwise, transform records and add those that match the createFilter
        final int remaining = limit - mtResult.getRecords().size();
        final List<Record> innerMtRecords = new ArrayList<>(remaining);
        final int consumed = addRecords(innerMtRecords, remaining, records, recordFilter, recordMapper);

        // If we consumed all records, then we can return the records and next shard iterator
        if (consumed == records.size()) {
            mtResult.setNextShardIterator(result.getNextShardIterator());
            if (!records.isEmpty()) {
                mtResult.getRecords().addAll(innerMtRecords);
                mtResult.setLastSequenceNumber(getLast(records).getDynamodb().getSequenceNumber());
            }
            return records.size();
        } else {
            // If we did not consume all records, then the loaded segment contains more tenant records than what we can
            // return per the client-specified limit. In that case we cannot use the loaded result, since the next shard
            // iterator would skip the records that were not returned. Therefore, we retry the load request with the
            // number of consumed records as the call limit, so that we get at most as many tenant records as needed for
            // the client-specified limit.
            return retryLoadRecords(mtResult, limit, recordFilter, recordMapper, consumed);
        }
    }

    // helper method for loadRecords (extracted to avoid accidentally referencing the wrong local variables)
    private int retryLoadRecords(MtGetRecordsResult mtResult,
                                 int limit,
                                 Predicate<Record> recordFilter,
                                 Function<Record, MtRecord> recordMapper,
                                 int innerLimit) {
        final GetRecordsRequest request = new GetRecordsRequest().withLimit(innerLimit)
            .withShardIterator(mtResult.getNextShardIterator());
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();
        mtResult.setNextShardIterator(result.getNextShardIterator());
        // shouldn't happen, but just to be safe
        if (!records.isEmpty()) {
            final int consumed = addRecords(mtResult.getRecords(), limit, records, recordFilter, recordMapper);
            checkState(consumed == records.size()); // can't happen unless stream order changes
            mtResult.setLastSequenceNumber(getLast(records).getDynamodb().getSequenceNumber());
        }
        return records.size();
    }

    /**
     * Transforms and filters {@code records} and adds them to {@code mtRecords} until the limit is reached. Returns
     * how many records were traversed before limit or end of collection was reached.
     */
    private static int addRecords(List<? super MtRecord> mtRecords,
                                  int limit,
                                  List<? extends Record> records,
                                  Predicate<Record> recordFilter,
                                  Function<Record, MtRecord> recordMapper) {
        int consumedRecords = 0;
        for (Record record : records) {
            if (recordFilter.test(record)) {
                if (mtRecords.size() >= limit) {
                    break;
                } else {
                    mtRecords.add(recordMapper.apply(record));
                }
            }
            consumedRecords++;
        }
        return consumedRecords;
    }

}
