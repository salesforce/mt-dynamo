/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.CONTEXT;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_TABLE_NAME;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getKeyMapper;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getTableMapping;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingTestUtil.buildTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Identity;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.HashPartitioningKeyPrefixFunction;
import org.junit.jupiter.api.Test;

class HashPartitioningRecordMapperTest {

    private static final DynamoTableDescription VIRTUAL_TABLE = buildTable(VIRTUAL_TABLE_NAME,
        new PrimaryKey(VIRTUAL_HK, S, VIRTUAL_RK, S));
    private static final HashPartitioningTableMapping TABLE_MAPPING = getTableMapping(VIRTUAL_TABLE);
    private static final RecordMapper MAPPER = TABLE_MAPPING.getRecordMapper();

    @Test
    void testFilter() {
        AttributeValue physicalHkValue = HashPartitioningKeyPrefixFunction.toPhysicalHashKey(
            CONTEXT, VIRTUAL_TABLE_NAME, 3);
        Record record = new Record().withDynamodb(new StreamRecord().withKeys(
            ImmutableMap.of(PHYSICAL_HK, physicalHkValue, PHYSICAL_RK, new AttributeValue() /*doesn't matter*/)));
        assertTrue(MAPPER.createFilter().test(record));
    }

    @Test
    void testFilterNegative() {
        AttributeValue physicalHkValue = HashPartitioningKeyPrefixFunction.toPhysicalHashKey(
            CONTEXT + "Other", VIRTUAL_TABLE_NAME, 3);
        Record record = new Record().withDynamodb(new StreamRecord().withKeys(
            ImmutableMap.of(PHYSICAL_HK, physicalHkValue, PHYSICAL_RK, new AttributeValue() /*doesn't matter*/)));
        assertFalse(MAPPER.createFilter().test(record));

        physicalHkValue = HashPartitioningKeyPrefixFunction.toPhysicalHashKey(
            CONTEXT, VIRTUAL_TABLE_NAME + "Other", 3);
        record = new Record().withDynamodb(new StreamRecord().withKeys(
            ImmutableMap.of(PHYSICAL_HK, physicalHkValue, PHYSICAL_RK, new AttributeValue() /*doesn't matter*/)));
        assertFalse(MAPPER.createFilter().test(record));
    }

    @Test
    void testMap() {
        final AttributeValue virtualHkVal = new AttributeValue("hkVal");
        final AttributeValue virtualRkVal = new AttributeValue("rkVal");
        final AttributeValue physicalHkVal = getKeyMapper(VIRTUAL_TABLE).toPhysicalHashKey(S, virtualHkVal);
        final AttributeValue physicalRkVal = HashPartitioningKeyMapper.toPhysicalRangeKey(
            VIRTUAL_TABLE.getPrimaryKey(), virtualHkVal, virtualRkVal);

        final String attrName = "someField";
        final AttributeValue oldAttrVal = new AttributeValue("old");
        final AttributeValue newAttrVal = new AttributeValue("new");

        final Record record = new Record()
            .withEventID("e1")
            .withEventName(OperationType.INSERT)
            .withAwsRegion(Regions.US_EAST_1.getName())
            .withEventSource("aws:dynamodb")
            .withEventVersion("1.0")
            .withUserIdentity(new Identity().withPrincipalId("p").withType("Service"))
            .withDynamodb(
                new StreamRecord()
                    .withKeys(ImmutableMap.of(PHYSICAL_HK, physicalHkVal, PHYSICAL_RK, physicalRkVal))
                    .withOldImage(ImmutableMap.of(PHYSICAL_HK, physicalHkVal, PHYSICAL_RK, physicalRkVal,
                        attrName, oldAttrVal))
                    .withNewImage(ImmutableMap.of(PHYSICAL_HK, physicalHkVal, PHYSICAL_RK, physicalRkVal,
                        attrName, newAttrVal))
                    .withSequenceNumber("1234")
                    .withSizeBytes(10L)
                    .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
            );
        final Record expected = record.clone();
        expected.withDynamodb(expected.getDynamodb().clone());
        expected.getDynamodb()
            .withKeys(ImmutableMap.of(VIRTUAL_HK, virtualHkVal, VIRTUAL_RK, virtualRkVal))
            .withOldImage(ImmutableMap.of(VIRTUAL_HK, virtualHkVal, VIRTUAL_RK, virtualRkVal, attrName, oldAttrVal))
            .withNewImage(ImmutableMap.of(VIRTUAL_HK, virtualHkVal, VIRTUAL_RK, virtualRkVal, attrName, newAttrVal));

        assertEquals(expected, MAPPER.apply(record));
    }

}
