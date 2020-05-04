/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Maps physical stream records into virtual stream records. Also exposes a filter method to allow pushing tenant table
 * predicate as low as possible when traversing a shared stream.
 */
public class RecordMapper implements Function<Record, MtRecord> {

    private final String context;
    private final String virtualTableName;
    private final String physicalHashKey;
    private final Predicate<AttributeValue> isMatchingPhysicalHashKey;
    private final ItemMapper itemMapper;

    RecordMapper(String context,
                 String virtualTableName,
                 String physicalHashKey,
                 Predicate<AttributeValue> isMatchingPhysicalHashKey,
                 ItemMapper itemMapper) {
        this.context = checkNotNull(context);
        this.virtualTableName = virtualTableName;
        this.physicalHashKey = physicalHashKey;
        this.isMatchingPhysicalHashKey = isMatchingPhysicalHashKey;
        this.itemMapper = itemMapper;
    }

    public Predicate<Record> createFilter() {
        return this::isMatchingPhysicalRecord;
    }

    private boolean isMatchingPhysicalRecord(Record physicalRecord) {
        AttributeValue physicalHashKeyValue = physicalRecord.getDynamodb().getKeys().get(physicalHashKey);
        return isMatchingPhysicalHashKey.test(physicalHashKeyValue);
    }

    @Override
    public MtRecord apply(Record record) {
        final StreamRecord streamRecord = record.getDynamodb();
        return getDefaultMtRecord(record)
            .withContext(context)
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

    public static MtRecord getDefaultMtRecord(Record record) {
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
        return ret;
    }

}
