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
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBase;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import io.micrometer.core.instrument.Counter;
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

    private final Timer getRecordsTimer;
    private final Counter getRecordsCounter;
    private final Counter getRecordsLoadedCounter;

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
        getRecordsTimer = meterRegistry.timer(name + ".GetRecords.Timer");
        getRecordsCounter = meterRegistry.counter(name + ".GetRecords.Counter");
        getRecordsLoadedCounter = meterRegistry.counter(name + ".GetRecords.Loaded.Counter");
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return super.getShardIterator(request);
    }

    /**
     * Keeps fetching records until it reaches:
     * <ol>
     * <li>limit,</li>
     * <li>current or absolute end of shard, or</li>
     * <li>time bound</li>
     * </ol>
     * Returns a subtype of {@link GetRecordsResult} that includes the last consumed sequence number from the underlying
     * stream shard, so that clients can request a new shard iterator where they left off.
     */
    @Override
    protected MtGetRecordsResult getRecords(GetRecordsRequest request, Function<Record, MtRecord> recordMapper,
                                            Predicate<MtRecord> recordFilter) {
        return getRecordsTimer.record(() -> {
            Optional.ofNullable(request.getLimit()).ifPresent(limit -> checkArgument(limit > 0 && limit <= MAX_LIMIT));

            final int limit = Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT);
            final long timeLimit = mtDynamoDb.getGetRecordsTimeLimit();
            final long time = mtDynamoDb.getClock().millis();

            final MtGetRecordsResult result = new MtGetRecordsResult()
                .withRecords(new ArrayList<>(limit))
                .withNextShardIterator(request.getShardIterator());

            int recordsLoaded = 0;
            do {
                recordsLoaded += loadRecords(result, limit, recordMapper, recordFilter);
            } while (result.getRecords().size() < limit     // only continue if we need more tenant records,
                && recordsLoaded % MAX_LIMIT == 0           // have not reached current end of the underlying stream,
                && result.getNextShardIterator() != null    // have not reached absolute end of underlying stream,
                && (mtDynamoDb.getClock().millis() - time) <= timeLimit // and have not exceeded the soft time limit
            );

            getRecordsCounter.increment(result.getRecords().size());
            getRecordsLoadedCounter.increment(recordsLoaded);

            return result;
        });
    }

    /**
     * Fetches more records using the next iterator in the result and adds them to the result records. Returns the
     * number of records that were loaded from the underlying stream, so that the caller can decide whether to continue.
     */
    private int loadRecords(MtGetRecordsResult mtResult,
                            int limit,
                            Function<Record, MtRecord> recordMapper,
                            Predicate<MtRecord> recordFilter) {
        // retrieve max number of records from underlying stream (and retry below if we got too many)
        final GetRecordsRequest request = new GetRecordsRequest().withLimit(MAX_LIMIT)
            .withShardIterator(mtResult.getNextShardIterator());
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();

        // otherwise, transform records and add those that match the filter
        final int remaining = limit - mtResult.getRecords().size();
        final List<Record> innerMtRecords = new ArrayList<>(remaining);
        final int consumed = addRecords(innerMtRecords, remaining, records, recordMapper, recordFilter);

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
            return retryLoadRecords(mtResult, limit, recordMapper, recordFilter, consumed);
        }
    }

    // helper method for loadRecords (extracted to avoid accidentally referencing the wrong local variables)
    private int retryLoadRecords(MtGetRecordsResult mtResult,
                                 int limit,
                                 Function<Record, MtRecord> recordMapper,
                                 Predicate<MtRecord> recordFilter,
                                 int innerLimit) {
        final GetRecordsRequest request = new GetRecordsRequest().withLimit(innerLimit)
            .withShardIterator(mtResult.getNextShardIterator());
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();
        mtResult.setNextShardIterator(result.getNextShardIterator());
        // shouldn't happen, but just to be safe
        if (!records.isEmpty()) {
            final int consumed = addRecords(mtResult.getRecords(), limit, records, recordMapper, recordFilter);
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
                                  Function<Record, MtRecord> recordMapper,
                                  Predicate<MtRecord> recordFilter) {
        int consumedRecords = 0;
        for (Record record : records) {
            MtRecord mtRecord = recordMapper.apply(record);
            if (recordFilter.test(mtRecord)) {
                if (mtRecords.size() >= limit) {
                    break;
                } else {
                    mtRecords.add(mtRecord);
                }
            }
            consumedRecords++;
        }
        return consumedRecords;
    }

    @Override
    protected Predicate<MtRecord> getRecordFilter(StreamArn arn) {
        Predicate<MtRecord> defaultPredicate = super.getRecordFilter(arn);
        return mtRecord ->
            mtDynamoDb.getMtContext().withContext(mtRecord.getContext(), this::isStreamEnabled, mtRecord.getTableName())
                && defaultPredicate.test(mtRecord);
    }

    private boolean isStreamEnabled(String tableName) {
        return mtDynamoDb.getTableMapping(tableName).getVirtualTable().getStreamSpecification().isStreamEnabled();
    }

    @Override
    protected Function<Record, MtRecord> getRecordMapper(StreamArn arn) {
        Function<Map<String, AttributeValue>, FieldValue<?>> fieldValueFunction =
            mtDynamoDb.getFieldValueFunction(arn.getTableName());
        return record -> mapRecord(fieldValueFunction, record);
    }

    private MtRecord mapRecord(Function<Map<String, AttributeValue>, FieldValue<?>> fieldValueFunction,
                               Record record) {
        FieldValue<?> fieldValue = fieldValueFunction.apply(record.getDynamodb().getKeys());
        MtAmazonDynamoDbContextProvider mtContext = mtDynamoDb.getMtContext();
        // execute in record tenant context to get table mapping

        TableMapping tableMapping = mtContext.withContext(fieldValue.getContext(),
            mtDynamoDb::getTableMapping, fieldValue.getTableName());
        ItemMapper itemMapper = tableMapping.getItemMapper();
        StreamRecord streamRecord = record.getDynamodb();
        return new MtRecord()
            .withAwsRegion(record.getAwsRegion())
            .withEventID(record.getEventID())
            .withEventName(record.getEventName())
            .withEventSource(record.getEventSource())
            .withEventVersion(record.getEventVersion())
            .withContext(fieldValue.getContext())
            .withTableName(fieldValue.getTableName())
            .withDynamodb(new StreamRecord()
                .withKeys(itemMapper.reverse(streamRecord.getKeys()))
                .withNewImage(itemMapper.reverse(streamRecord.getNewImage()))
                .withOldImage(itemMapper.reverse(streamRecord.getOldImage()))
                .withSequenceNumber(streamRecord.getSequenceNumber())
                .withStreamViewType(streamRecord.getStreamViewType())
                .withApproximateCreationDateTime(streamRecord.getApproximateCreationDateTime())
                .withSizeBytes(streamRecord.getSizeBytes()));
    }

}
