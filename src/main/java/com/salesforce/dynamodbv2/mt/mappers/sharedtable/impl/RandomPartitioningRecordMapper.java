package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Predicate;

/**
 * Maps physical stream records into virtual stream records. Also exposes a filter method to allow pushing tenant table
 * predicate as low as possible when traversing a shared stream.
 */
class RandomPartitioningRecordMapper implements RecordMapper {

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final String virtualTableName;
    private final ItemMapper itemMapper;
    private final String hashKeyName;
    private final FieldMapper fieldMapper;

    RandomPartitioningRecordMapper(MtAmazonDynamoDbContextProvider mtContext,
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

    @Override
    public Predicate<Record> createFilter() {
        final Predicate<AttributeValue> filter = fieldMapper.createFilter();
        return record -> filter.test(record.getDynamodb().getKeys().get(hashKeyName));
    }

    @Override
    public MtRecord apply(Record record) {
        final StreamRecord streamRecord = record.getDynamodb();
        final MtRecord ret = new MtRecord();
        if (record.getAwsRegion() != null) {
            ret.withAwsRegion(record.getAwsRegion());
        }

        if (record.getEventID() != null) {
            ret.withEventID(record.getEventID());
        }

        if (record.getEventName() != null) {
            ret.withEventName(record.getEventName());
        }

        if (record.getEventSource() != null) {
            ret.withEventSource(record.getEventSource());
        }

        if (record.getEventVersion() != null) {
            ret.withEventVersion(record.getEventVersion());
        }

        if (record.getUserIdentity() != null) {
            ret.withUserIdentity(record.getUserIdentity());
        }
        return ret.withContext(mtContext.getContext())
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
