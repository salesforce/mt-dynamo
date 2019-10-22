/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.DELIMITER;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.PhysicalRangeKeyBytesConverter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

class HashPartitioningTestUtil {

    private static final String CONTEXT = "someContext";
    static final int NUM_BUCKETS = 10;

    static final String VIRTUAL_TABLE_NAME = "someVirtualTable";
    static final String VIRTUAL_HK = "virtualHk";
    static final String VIRTUAL_GSI = "virtualGsi";
    static final String VIRTUAL_GSI_HK = "virtualGsiHk";
    static final String VIRTUAL_RK = "virtualRk";
    static final String VIRTUAL_GSI_RK = "virtualGsiRk";
    static final String SOME_FIELD = "someField";
    static final String PHYSICAL_HK = "physicalHk";
    static final String PHYSICAL_RK = "physicalRk";
    private static final String PHYSICAL_GSI = "physicalGsi";
    static final String PHYSICAL_GSI_HK = "physicalGsiHk";
    static final String PHYSICAL_GSI_RK = "physicalGsiRk";

    private static final DynamoTableDescription PHYSICAL_TABLE = TableMappingTestUtil.buildTable(
        "somePhysicalTable",
        new PrimaryKey(PHYSICAL_HK, B, PHYSICAL_RK, B),
        ImmutableMap.of(PHYSICAL_GSI, new PrimaryKey(PHYSICAL_GSI_HK, B, PHYSICAL_GSI_RK, B))
    );

    static HashPartitioningTableMapping getTableMapping(DynamoTableDescription virtualTable) {
        return new HashPartitioningTableMapping(virtualTable, PHYSICAL_TABLE,
            index -> index.getIndexName().equals(VIRTUAL_GSI) ? PHYSICAL_TABLE.findSi(PHYSICAL_GSI) : null,
            () -> Optional.of(CONTEXT), NUM_BUCKETS);
    }

    static HashPartitioningKeyMapper getKeyMapper(DynamoTableDescription virtualTable) {
        return new HashPartitioningKeyMapper(virtualTable.getTableName(), () -> Optional.of(CONTEXT), NUM_BUCKETS);
    }

    static DynamoTableDescription buildVirtualHkTable(ScalarAttributeType hashKeyType) {
        return TableMappingTestUtil.buildTable(
            VIRTUAL_TABLE_NAME,
            new PrimaryKey(VIRTUAL_HK, hashKeyType),
            ImmutableMap.of(VIRTUAL_GSI, new PrimaryKey(VIRTUAL_GSI_HK, hashKeyType))
        );
    }

    static DynamoTableDescription buildVirtualHkRkTable(ScalarAttributeType hkType, ScalarAttributeType rkType,
                                                        ScalarAttributeType gsiHkType, ScalarAttributeType gsiRkType) {
        return TableMappingTestUtil.buildTable(
            VIRTUAL_TABLE_NAME,
            new PrimaryKey(VIRTUAL_HK, hkType, VIRTUAL_RK, rkType),
            ImmutableMap.of(VIRTUAL_GSI,
                new PrimaryKey(VIRTUAL_GSI_HK, gsiHkType, VIRTUAL_GSI_RK, gsiRkType))
        );
    }

    static AttributeValue getPhysicalHkValue(TestHashKeyValue value) {
        String asString = CONTEXT + DELIMITER + VIRTUAL_TABLE_NAME + DELIMITER
            + (value.getRawValueHashCode() % NUM_BUCKETS);
        return new AttributeValue().withB(ByteBuffer.wrap(asString.getBytes(StandardCharsets.UTF_8)));
    }

    static AttributeValue getPhysicalRkValue(ScalarAttributeType type, AttributeValue attributeValue) {
        ByteBuffer bytes = PhysicalRangeKeyBytesConverter.toBytes(type, attributeValue);
        return new AttributeValue().withB(bytes);
    }

    static AttributeValue getPhysicalRkValue(ScalarAttributeType hkType, AttributeValue hkAttributeValue,
                                             ScalarAttributeType rkType, AttributeValue rkAttributeValue) {
        ByteBuffer bytes = PhysicalRangeKeyBytesConverter.toBytes(hkType, hkAttributeValue, rkType, rkAttributeValue);
        return new AttributeValue().withB(bytes);
    }

    static class TestHashKeyValue {

        private final AttributeValue attributeValue;
        private final int rawValueHashCode;

        TestHashKeyValue(String value) {
            this(new AttributeValue().withS(value), value.hashCode());
        }

        TestHashKeyValue(BigDecimal value) {
            this(new AttributeValue().withN(value.toString()), value.hashCode());
        }

        TestHashKeyValue(byte[] value) {
            this(new AttributeValue().withB(ByteBuffer.wrap(value)), Arrays.hashCode(value));
        }

        TestHashKeyValue(AttributeValue attributeValue, int rawValueHashCode) {
            this.attributeValue = attributeValue;
            this.rawValueHashCode = rawValueHashCode;
        }

        AttributeValue getAttributeValue() {
            return attributeValue;
        }

        int getRawValueHashCode() {
            return rawValueHashCode;
        }
    }
}
