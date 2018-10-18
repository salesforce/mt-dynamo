package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toCollection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.base.Preconditions;
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
import java.util.stream.Collectors;


public class MtAmazonDynamoDbStreamsBySharedTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbBySharedTable> {

    private static final int MAX_LIMIT = 1000;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
        MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams, mtDynamoDb);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return super.getShardIterator(request);
    }

    // keeps going until it either reaches the end of stream or finds a record. This is so clients
    // that don't cache shard iterators are guaranteed to make progress eventually.
    @Override
    protected GetRecordsResult getRecords(GetRecordsRequest request, Function<Record, MtRecord> recordMapper,
        Predicate<MtRecord> recordFilter) {
        Optional.ofNullable(request.getLimit()).ifPresent(limit -> checkArgument(limit > 0 && limit <= MAX_LIMIT));

        List<Record> mtRecords = new ArrayList<>(Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT));
        String iterator = request.getShardIterator();

        // keep fetching records until we reach the (current) end or find at least one record for this tenant
        do {
            GetRecordsResult result = dynamoDbStreams.getRecords(request.clone().withShardIterator(iterator));
            iterator = result.getNextShardIterator();
            List<Record> records = result.getRecords();
            if (records.isEmpty()) {
                break;
            }
            records.stream().map(recordMapper).filter(recordFilter).collect(toCollection(() -> mtRecords));
        } while (mtRecords.isEmpty() && iterator != null);

        return new GetRecordsResult().withRecords(mtRecords).withNextShardIterator(iterator);
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
