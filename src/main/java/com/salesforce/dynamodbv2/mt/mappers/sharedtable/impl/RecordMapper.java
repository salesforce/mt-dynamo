package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Function;
import java.util.function.Predicate;

public class RecordMapper implements Function<Record, MtRecord> {

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final String virtualTableName;
    private final ItemMapper itemMapper;
    private final String hashKeyName;
    private final FieldMapper fieldMapper;

    RecordMapper(MtAmazonDynamoDbContextProvider mtContext,
                 String virtualTableName,
                 ItemMapper itemMapper,
                 FieldMapper fieldMapper,
                 String hashKeyName) {
        this.mtContext = mtContext;
        this.virtualTableName = virtualTableName;
        this.itemMapper = itemMapper;
        this.hashKeyName = hashKeyName;
        this.fieldMapper = fieldMapper;
    }

    public Predicate<Record> createFilter() {
        final Predicate<AttributeValue> filter = fieldMapper.createFilter();
        return record -> filter.test(record.getDynamodb().getKeys().get(hashKeyName));
    }

    @Override
    public MtRecord apply(Record record) {
        final StreamRecord streamRecord = record.getDynamodb();
        return new MtRecord()
            .withAwsRegion(record.getAwsRegion())
            .withEventID(record.getEventID())
            .withEventName(record.getEventName())
            .withEventSource(record.getEventSource())
            .withEventVersion(record.getEventVersion())
            .withUserIdentity(record.getUserIdentity())
            .withContext(mtContext.getContext())
            .withTableName(virtualTableName)
            .withDynamodb(new StreamRecord()
                .withKeys(itemMapper.reverse(streamRecord.getKeys())) // should this use key mapper?
                .withNewImage(itemMapper.reverse(streamRecord.getNewImage()))
                .withOldImage(itemMapper.reverse(streamRecord.getOldImage()))
                .withSequenceNumber(streamRecord.getSequenceNumber())
                .withStreamViewType(streamRecord.getStreamViewType())
                .withApproximateCreationDateTime(streamRecord.getApproximateCreationDateTime())
                .withSizeBytes(streamRecord.getSizeBytes()));
    }
}
