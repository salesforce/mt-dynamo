/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningItemMapper.AttributeBytesConverter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HashPartitioningItemMapperTest {

    private static final char DELIMITER = '/';
    private static final String CONTEXT = "someContext";
    private static final int NUM_BUCKETS = 10;
    private static final String VIRTUAL_TABLE_NAME = "someVirtualTable";

    private static final DynamoTableDescription VIRTUAL_HK_TABLE = new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder
            .builder()
            .withTableName(VIRTUAL_TABLE_NAME)
            .withTableKeySchema("virtualHk", S)
            .addSi("virtualGsi", GSI, new PrimaryKey("virtualGsiHk", S), 1L)
            .build());

    private static final DynamoTableDescription VIRTUAL_HK_RK_TABLE = new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder
            .builder()
            .withTableName(VIRTUAL_TABLE_NAME)
            .withTableKeySchema("virtualHk", S, "virtualRk", N)
            .addSi("virtualGsi", GSI, new PrimaryKey("virtualGsiHk", S, "virtualGsiRk", B), 1L)
            .build());

    private static final DynamoTableDescription PHYSICAL_TABLE = new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder
            .builder()
            .withTableName("somePhysicalTable")
            .withTableKeySchema("physicalHk", B, "physicalRk", B)
            .addSi("physicalGsi", GSI, new PrimaryKey("physicalGsiHk", B, "physicalGsiRk", B), 1L)
            .build());

    private static final AttributeValue VIRTUAL_HK_VALUE = new AttributeValue().withS("hkValue");
    private static final AttributeValue VIRTUAL_RK_VALUE = new AttributeValue().withN("123.456");
    private static final AttributeValue VIRTUAL_GSI_HK_VALUE = new AttributeValue().withS("gsiHkValue");
    private static final AttributeValue VIRTUAL_GSI_RK_VALUE = new AttributeValue().withB(
        ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));
    private static final AttributeValue SOME_VALUE = new AttributeValue().withS("someValue");

    @Test
    void applyAndReverse_hkTable_noGsiValue() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "someField", SOME_VALUE);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE),
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @Test
    void applyAndReverse_hkTable_withGsiValue() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "someField", SOME_VALUE);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        String expectedPhysicalGsiHk = getExpectedPhysicalHashKey(VIRTUAL_GSI_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE),
            "physicalGsiHk", getAttributeValueInBytes(expectedPhysicalGsiHk),
            "physicalGsiRk", getAttributeValueInBytes(S, VIRTUAL_GSI_HK_VALUE),
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @Test
    void applyAndReverse_hkRkTable_noGsiValues() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_RK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualRk", VIRTUAL_RK_VALUE,
            "someField", SOME_VALUE);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE, N, VIRTUAL_RK_VALUE),
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @Test
    void applyAndReverse_hkRkTable_withGsiValues() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_RK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualRk", VIRTUAL_RK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "virtualGsiRk", VIRTUAL_GSI_RK_VALUE,
            "someField", SOME_VALUE);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        String expectedPhysicalGsiHk = getExpectedPhysicalHashKey(VIRTUAL_GSI_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE, N, VIRTUAL_RK_VALUE),
            "physicalGsiHk", getAttributeValueInBytes(expectedPhysicalGsiHk),
            "physicalGsiRk", getAttributeValueInBytes(S, VIRTUAL_GSI_HK_VALUE, B, VIRTUAL_GSI_RK_VALUE),
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @Test
    void applyAndReverse_hkRkTable_notAllGsiValues() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_RK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualRk", VIRTUAL_RK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "someField", SOME_VALUE);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE, N, VIRTUAL_RK_VALUE),
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @Test
    void applyToKeyAttributes_hkTable() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyToKeyAttributes(item, null);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE));
        assertEquals(expectedMappedItem, mappedItem);
    }

    @Test
    void applyToKeyAttributes_hkTableAndGsi() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyToKeyAttributes(item,
            VIRTUAL_HK_TABLE.findSi("virtualGsi"));

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        String expectedPhysicalGsiHk = getExpectedPhysicalHashKey(VIRTUAL_GSI_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE),
            "physicalGsiHk", getAttributeValueInBytes(expectedPhysicalGsiHk),
            "physicalGsiRk", getAttributeValueInBytes(S, VIRTUAL_GSI_HK_VALUE));
        assertEquals(expectedMappedItem, mappedItem);
    }

    @Test
    void applyToKeyAttributes_hkRkTable() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_RK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualRk", VIRTUAL_RK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "virtualGsiRk", VIRTUAL_GSI_RK_VALUE,
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyToKeyAttributes(item, null);

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE, N, VIRTUAL_RK_VALUE));
        assertEquals(expectedMappedItem, mappedItem);
    }

    @Test
    void applyToKeyAttributes_hkRkTableAndGsi() {
        HashPartitioningItemMapper itemMapper = getItemMapper(VIRTUAL_HK_RK_TABLE);

        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", VIRTUAL_HK_VALUE,
            "virtualRk", VIRTUAL_RK_VALUE,
            "virtualGsiHk", VIRTUAL_GSI_HK_VALUE,
            "virtualGsiRk", VIRTUAL_GSI_RK_VALUE,
            "someField", SOME_VALUE);

        Map<String, AttributeValue> mappedItem = itemMapper.applyToKeyAttributes(item,
            VIRTUAL_HK_RK_TABLE.findSi("virtualGsi"));

        String expectedPhysicalHk = getExpectedPhysicalHashKey(VIRTUAL_HK_VALUE.getS());
        String expectedPhysicalGsiHk = getExpectedPhysicalHashKey(VIRTUAL_GSI_HK_VALUE.getS());
        Map<String, AttributeValue> expectedMappedItem = ImmutableMap.of(
            "physicalHk", getAttributeValueInBytes(expectedPhysicalHk),
            "physicalRk", getAttributeValueInBytes(S, VIRTUAL_HK_VALUE, N, VIRTUAL_RK_VALUE),
            "physicalGsiHk", getAttributeValueInBytes(expectedPhysicalGsiHk),
            "physicalGsiRk", getAttributeValueInBytes(S, VIRTUAL_GSI_HK_VALUE, B, VIRTUAL_GSI_RK_VALUE));
        assertEquals(expectedMappedItem, mappedItem);
    }

    @Test
    void reverseNull() {
        assertNull(getItemMapper(VIRTUAL_HK_TABLE).reverse(null));
    }

    private AttributeValue getAttributeValueInBytes(String stringValue) {
        return getAttributeValueInBytes(S, new AttributeValue().withS(stringValue));
    }

    private AttributeValue getAttributeValueInBytes(ScalarAttributeType type, AttributeValue attributeValue) {
        ByteBuffer bytes = AttributeBytesConverter.toBytes(type, attributeValue);
        return new AttributeValue().withB(bytes);
    }

    private AttributeValue getAttributeValueInBytes(ScalarAttributeType hkType, AttributeValue hkAttributeValue,
                                                    ScalarAttributeType rkType, AttributeValue rkAttributeValue) {
        ByteBuffer bytes = AttributeBytesConverter.toBytes(hkType, hkAttributeValue, rkType, rkAttributeValue);
        return new AttributeValue().withB(bytes);
    }

    private String getExpectedPhysicalHashKey(Object hashKeyValue) {
        return CONTEXT + DELIMITER + VIRTUAL_TABLE_NAME + DELIMITER + (hashKeyValue.hashCode() % NUM_BUCKETS);
    }

    private HashPartitioningItemMapper getItemMapper(DynamoTableDescription virtualTable) {
        return new HashPartitioningItemMapper(
            virtualTable,
            PHYSICAL_TABLE,
            index -> index.getIndexName().equals("virtualGsi") ? PHYSICAL_TABLE.findSi("physicalGsi") : null,
            () -> Optional.of(CONTEXT),
            NUM_BUCKETS,
            DELIMITER
        );
    }

}
