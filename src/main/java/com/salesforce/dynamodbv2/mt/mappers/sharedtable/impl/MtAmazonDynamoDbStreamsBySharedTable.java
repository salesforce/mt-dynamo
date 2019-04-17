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
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class MtAmazonDynamoDbStreamsBySharedTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbBySharedTable> {

    private static final int MAX_LIMIT = 1000;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb      corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
                                                MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams, mtDynamoDb);
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
        Optional.ofNullable(request.getLimit()).ifPresent(limit -> checkArgument(limit > 0 && limit <= MAX_LIMIT));

        final int limit = Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT);
        final long timeLimit = mtDynamoDb.getGetRecordsTimeLimit();
        final long time = mtDynamoDb.getClock().millis();

        final MtGetRecordsResult result = new MtGetRecordsResult()
            .withRecords(new ArrayList<>(limit))
            .withNextShardIterator(request.getShardIterator())
            .withLastSequenceNumber(null);

        while (result.getRecords().size() < limit
            && result.getNextShardIterator() != null
            && (mtDynamoDb.getClock().millis() - time) < timeLimit) {
            if (!addRecords(result, limit, recordMapper, recordFilter)) {
                break;
            }
        }

        return result;
    }

    /**
     * Fetches more records using the next iterator in the result and adds them to the result records. Returns whether
     * to keep going or not.
     */
    private boolean addRecords(MtGetRecordsResult result,
                               int limit,
                               Function<Record, MtRecord> recordMapper,
                               Predicate<MtRecord> recordFilter) {
        // retrieve max number of records from underlying stream (and retry below if we got too many)
        final GetRecordsRequest innerRequest = new GetRecordsRequest().withLimit(MAX_LIMIT)
            .withShardIterator(result.getNextShardIterator());
        GetRecordsResult innerResult = dynamoDbStreams.getRecords(innerRequest);
        result.setNextShardIterator(innerResult.getNextShardIterator());
        List<Record> records = innerResult.getRecords();
        // if we did not any records, no point continuing
        if (records.isEmpty()) {
            return false;
        }

        // otherwise, transform records and add those that match the filter
        final int innerLimit = limit - result.getRecords().size();
        final List<Record> innerMtRecords = new ArrayList<>(innerLimit);
        int consumedRecords = addRecords(innerMtRecords, innerLimit, records, recordMapper, recordFilter);

        // If we consumed all records, then we did not exceed the limit, so worth continuing (if we have time)
        if (consumedRecords == records.size()) {
            result.getRecords().addAll(innerMtRecords);
            result.setLastSequenceNumber(getLast(records).getDynamodb().getSequenceNumber());
            return true;
        } else {
            // If we exceeded limit: retry with lower limit (can't just return a subset of records, because the next
            // shard iterator would not align with the last returned record)
            innerResult = dynamoDbStreams.getRecords(innerRequest.withLimit(consumedRecords));
            result.setNextShardIterator(innerResult.getNextShardIterator());
            records = innerResult.getRecords();
            // shouldn't happen, but just to be safe
            if (!records.isEmpty()) {
                consumedRecords = addRecords(result.getRecords(), limit, records, recordMapper, recordFilter);
                checkState(consumedRecords == records.size()); // can't happen unless stream order changes
                result.setLastSequenceNumber(getLast(records).getDynamodb().getSequenceNumber());
            }
            // either way we'll stop here (even if retrying didn't return sufficient records for some reason)
            return false;
        }
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
        Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction =
            mtDynamoDb.getFieldValueFunction(arn.getTableName());
        return record -> mapRecord(fieldValueFunction, record);
    }

    private MtRecord mapRecord(Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction,
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

}
