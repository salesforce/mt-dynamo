/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.SOME_FIELD;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkRkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getPhysicalHkValue;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getPhysicalRkValue;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getTableMapping;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingTestUtil.verifyApplyToUpdate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.AbstractConditionMapper.UpdateActions;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.AbstractQueryAndScanMapper.QueryRequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.TestHashKeyValue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashPartitioningConditionMapperTest {

    @Test
    void testParseUpdateExpression() {
        AttributeValue gsiHkValue = new AttributeValue().withS("gsiHkValue");
        AttributeValue gsiRkValue = new AttributeValue().withS("gsiRkValue");

        UpdateItemRequest request = new UpdateItemRequest()
            .withUpdateExpression("SET " + VIRTUAL_GSI_HK + " = :value1, #field = :value2")
            .addExpressionAttributeNamesEntry("#field", VIRTUAL_GSI_RK)
            .addExpressionAttributeValuesEntry(":value1", gsiHkValue)
            .addExpressionAttributeValuesEntry(":value2", gsiRkValue);

        RequestWrapper requestWrapper = new HashPartitioningConditionMapper.UpdateExpressionRequestWrapper(request);
        Map<String, AttributeValue> result = AbstractConditionMapper.parseUpdateExpression(requestWrapper,
            null).getSetActions();
        assertEquals(ImmutableMap.of(VIRTUAL_GSI_HK, gsiHkValue, VIRTUAL_GSI_RK, gsiRkValue), result);
    }

    @Test
    void testValidateFieldCanBeUpdated() {
        DynamoTableDescription virtualTable = TableMappingTestUtil.buildTable(
            "virtualTableName",
            new PrimaryKey(VIRTUAL_HK, S, VIRTUAL_RK, S),
            ImmutableMap.of("gsi1", new PrimaryKey("A", S, "B", S),
                "gsi2", new PrimaryKey("A", S, "C", S),
                "gsi3", new PrimaryKey("D", S, "B", S),
                "gsi4", new PrimaryKey("E", S, "F", S))
        );
        HashPartitioningTableMapping tableMapping = getTableMapping(virtualTable);
        HashPartitioningConditionMapper mapper = tableMapping.getConditionMapper();

        // cannot update table primary key fields
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of(VIRTUAL_HK));
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of(VIRTUAL_RK));
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of(VIRTUAL_HK, VIRTUAL_RK));

        // can update a field only if all other index partner fields are updated
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of("A", "B"));
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of("A", "C"));
        validateFieldsCanBeUpdatedError(mapper, ImmutableSet.of("A", "B", "C"));

        // TODO DG - update this test
        // valid field combinations
        // mapper.validateFieldsCanBeUpdated(ImmutableSet.of("A", "B", "C", "D"));
        // mapper.validateFieldsCanBeUpdated(ImmutableSet.of("E", "F"));
        // mapper.validateFieldsCanBeUpdated(Collections.emptySet());
    }

    private void validateFieldsCanBeUpdatedError(HashPartitioningConditionMapper mapper, Set<String> allSetFields) {
        try {
           // mapper.validateFieldsCanBeUpdated(allSetFields);
            fail("validateFieldsCanBeUpdated() should have thrown exception");
        } catch (AmazonServiceException | IllegalArgumentException e) {
            // expected
        }
    }

    @ParameterizedTest(name = "{index}")
    @MethodSource("getApplyToUpdateTestInputs")
    void testApplyToUpdateExpression(DynamoTableDescription virtualTable,
                                     UpdateItemRequest request,
                                     Map<String, AttributeValue> expectedUpdateItem,
                                     Map<String, String> conditionExpressionFieldPlaceholders,
                                     Map<String, AttributeValue> conditionExpressionValuePlaceholders) {
        HashPartitioningTableMapping tableMapping = getTableMapping(virtualTable);
        HashPartitioningConditionMapper mapper = tableMapping.getConditionMapper();
        mapper.applyForUpdate(request);

        verifyApplyToUpdate(request, expectedUpdateItem, conditionExpressionFieldPlaceholders,
            conditionExpressionValuePlaceholders);
    }

    private static Stream<Arguments> getApplyToUpdateTestInputs() {
        return ApplyToUpdateTestInputs.get();
    }

    private static class ApplyToUpdateTestInputs {

        private static final ScalarAttributeType HK_TYPE = S;
        private static final ScalarAttributeType RK_TYPE = S;
        private static final DynamoTableDescription HK_TABLE = buildVirtualHkTable(HK_TYPE);
        private static final DynamoTableDescription HK_RK_TABLE =
            buildVirtualHkRkTable(HK_TYPE, RK_TYPE, HK_TYPE, RK_TYPE);

        private static final String VIRTUAL_GSI_HK_RAW_VALUE = "virtualGsiHkValue";
        private static final TestHashKeyValue VIRTUAL_GSI_HK_VALUE = new TestHashKeyValue(VIRTUAL_GSI_HK_RAW_VALUE);
        private static final String VIRTUAL_GSI_RK_RAW_VALUE = "virtualGsiRkValue";
        private static final TestHashKeyValue VIRTUAL_GSI_RK_VALUE = new TestHashKeyValue(VIRTUAL_GSI_RK_RAW_VALUE);
        private static final AttributeValue SOME_FIELD_VALUE = new AttributeValue("someValue");

        private static Stream<Arguments> get() {
            return Stream.of(
                // update HK table
                Arguments.of(HK_TABLE,
                    toUpdateItemRequest(String.format("SET %s = :value1, %s = :value2", SOME_FIELD, VIRTUAL_GSI_HK),
                        null,
                        null,
                        ImmutableMap.of(":value1", SOME_FIELD_VALUE,
                            ":value2", VIRTUAL_GSI_HK_VALUE.getAttributeValue())),
                    ImmutableMap.of(SOME_FIELD, SOME_FIELD_VALUE,
                        VIRTUAL_GSI_HK, VIRTUAL_GSI_HK_VALUE.getAttributeValue(),
                        PHYSICAL_GSI_HK, getPhysicalHkValue(VIRTUAL_GSI_HK_VALUE),
                        PHYSICAL_GSI_RK, getPhysicalRkValue(HK_TYPE, VIRTUAL_GSI_HK_VALUE.getAttributeValue())),
                    null, null
                ),
                // update HK-RK table, with mix of field literals and placeholders
                Arguments.of(HK_RK_TABLE,
                    toUpdateItemRequest(String.format("SET %s = :value1, #field1 = :value2, %s = :value3",
                        SOME_FIELD, VIRTUAL_GSI_RK),
                        null,
                        ImmutableMap.of("#field1", VIRTUAL_GSI_HK),
                        ImmutableMap.of(":value1", SOME_FIELD_VALUE,
                            ":value2", VIRTUAL_GSI_HK_VALUE.getAttributeValue(),
                            ":value3", VIRTUAL_GSI_RK_VALUE.getAttributeValue())),
                    ImmutableMap.of(SOME_FIELD, SOME_FIELD_VALUE,
                        VIRTUAL_GSI_HK, VIRTUAL_GSI_HK_VALUE.getAttributeValue(),
                        VIRTUAL_GSI_RK, VIRTUAL_GSI_RK_VALUE.getAttributeValue(),
                        PHYSICAL_GSI_HK, getPhysicalHkValue(VIRTUAL_GSI_HK_VALUE),
                        PHYSICAL_GSI_RK, getPhysicalRkValue(HK_TYPE, VIRTUAL_GSI_HK_VALUE.getAttributeValue(),
                            RK_TYPE, VIRTUAL_GSI_RK_VALUE.getAttributeValue())),
                    null, null
                ),
                // update with some field and value placeholders appearing in condition expression
                Arguments.of(HK_TABLE,
                    toUpdateItemRequest("SET #field1 = :value1",
                        "#field1 <> :value1",
                        ImmutableMap.of("#field1", VIRTUAL_GSI_HK),
                        ImmutableMap.of(":value1", VIRTUAL_GSI_HK_VALUE.getAttributeValue())),
                    ImmutableMap.of(VIRTUAL_GSI_HK, VIRTUAL_GSI_HK_VALUE.getAttributeValue(),
                        PHYSICAL_GSI_HK, getPhysicalHkValue(VIRTUAL_GSI_HK_VALUE),
                        PHYSICAL_GSI_RK, getPhysicalRkValue(HK_TYPE, VIRTUAL_GSI_HK_VALUE.getAttributeValue())),
                    ImmutableMap.of("#field1", VIRTUAL_GSI_HK),
                    ImmutableMap.of(":value1", VIRTUAL_GSI_HK_VALUE.getAttributeValue())
                )
            );
        }

        private static UpdateItemRequest toUpdateItemRequest(String updateExpression, String conditionExpression,
                                                             Map<String, String> fieldPlaceholders,
                                                             Map<String, AttributeValue> valuePlaceholders) {
            return new UpdateItemRequest()
                .withUpdateExpression(updateExpression)
                .withConditionExpression(conditionExpression)
                .withExpressionAttributeNames(fieldPlaceholders)
                .withExpressionAttributeValues(valuePlaceholders);
        }
    }

    @ParameterizedTest(name = "{index}")
    @MethodSource("buildKeyConditionTestInputs")
    void testApplyToKeyCondition(DynamoTableDescription virtualTable, boolean useSecondaryIndex,
                                 @Nullable String rkExpression, Map<String, AttributeValue> rkValues,
                                 String expectedRkExpression, Map<String, AttributeValue> expectedRkValues) {
        String expression = (useSecondaryIndex ? VIRTUAL_GSI_HK : VIRTUAL_HK) + " = :vhk";
        if (rkExpression != null) {
            expression += " AND " + rkExpression;
        }
        Map<String, AttributeValue> values = new HashMap<>(rkValues);
        values.put(":vhk", KeyConditionTestInputs.VIRTUAL_HK_VALUE.getAttributeValue());

        String expectedExpression = "#field1 = :value1 AND " + expectedRkExpression;
        Map<String, String> expectedFields = ImmutableMap.of(
            "#field1", useSecondaryIndex ? PHYSICAL_GSI_HK : PHYSICAL_HK,
            "#field2", useSecondaryIndex ? PHYSICAL_GSI_RK : PHYSICAL_RK);
        Map<String, AttributeValue> expectedValues = new HashMap<>(expectedRkValues);
        expectedValues.put(":value1", getPhysicalHkValue(KeyConditionTestInputs.VIRTUAL_HK_VALUE));

        runApplyToKeyConditionTest(virtualTable, useSecondaryIndex, expression, null /*fields*/, values,
            expectedExpression, expectedFields, expectedValues);
    }

    private void runApplyToKeyConditionTest(DynamoTableDescription virtualTable, boolean useSecondaryIndex,
                                            String expression, Map<String, String> fields,
                                            Map<String, AttributeValue> values,
                                            String expectedExpression, Map<String, String> expectedFields,
                                            Map<String, AttributeValue> expectedValues) {
        HashPartitioningTableMapping tableMapping = getTableMapping(virtualTable);
        HashPartitioningConditionMapper mapper = tableMapping.getConditionMapper();

        QueryRequest request = new QueryRequest()
            .withKeyConditionExpression(expression)
            .withExpressionAttributeNames(fields)
            .withExpressionAttributeValues(values);
        RequestWrapper requestWrapper = new QueryRequestWrapper(request, request::getKeyConditionExpression,
            request::setKeyConditionExpression);
        RequestIndex requestIndex = tableMapping.getRequestIndex(useSecondaryIndex ? VIRTUAL_GSI : null);
        mapper.applyToKeyCondition(requestWrapper, requestIndex, request.getFilterExpression());

        assertEquals(expectedExpression, request.getKeyConditionExpression());
        assertEquals(expectedFields, request.getExpressionAttributeNames());
        assertEquals(expectedValues, request.getExpressionAttributeValues());
    }

    private static Stream<Arguments> buildKeyConditionTestInputs() {
        return KeyConditionTestInputs.get();
    }

    private static class KeyConditionTestInputs {

        private static final ScalarAttributeType HK_TYPE = B;
        private static final ScalarAttributeType RK_TYPE = B;
        private static final DynamoTableDescription VIRTUAL_HK_TABLE = buildVirtualHkTable(HK_TYPE);
        private static final DynamoTableDescription VIRTUAL_HK_RK_TABLE =
            buildVirtualHkRkTable(HK_TYPE, RK_TYPE, HK_TYPE, RK_TYPE);
        private static final byte[] VIRTUAL_HK_BYTES = new byte[] { 1, 2, 3 };
        private static final TestHashKeyValue VIRTUAL_HK_VALUE = new TestHashKeyValue(VIRTUAL_HK_BYTES);
        private static final int MAX_VIRTUAL_RK_LENGTH = 1024 - 2 - VIRTUAL_HK_BYTES.length;

        private static Stream<Arguments> get() {
            Stream.Builder<Arguments> inputs = Stream.builder();
            for (Boolean useSecondaryIndex : ImmutableList.of(Boolean.FALSE, Boolean.TRUE)) {
                String virtualRkField = useSecondaryIndex ? VIRTUAL_GSI_RK : VIRTUAL_RK;
                // HK table: "vhk = :vhk"
                inputs.add(Arguments.of(VIRTUAL_HK_TABLE, useSecondaryIndex,
                    null, Collections.emptyMap(),
                    "#field2 = :value2",
                    ImmutableMap.of(":value2", getPhysicalRkValue(HK_TYPE, VIRTUAL_HK_VALUE.getAttributeValue()))));
                // HK RK table: "vhk = :vhk AND vrk = :vrk"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                    virtualRkField + " = :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6 })),
                    "#field2 = :value2",
                    ImmutableMap.of(":value2", getPhysicalRkValueFromVirtualRk(new byte[] { 4, 5, 6 }))));
                // HK RK table; "vhk = :vhk"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                    null, Collections.emptyMap(),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[0],
                        ":value3", getBytesFilledWithMax(new byte[0]))));
                // HK RK table: "vhk = :vhk AND vrk < :vrk" (flavor 1: vrk ends with 0)
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " < :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6, 0, 0 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[0],
                        ":value3", new byte[] { 4, 5, 6, 0 })));
                // HK RK table: "vhk = :vhk AND vrk < :vrk" (flavor 2: vrk ends with nonzero)
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " < :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[0],
                        ":value3", getBytesFilledWithMax(new byte[] { 4, 5, 5 }))));
                // HK RK table: "vhk = :vhk AND vrk <= :vrk"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " <= :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6, 0, 0 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[0],
                        ":value3", new byte[] { 4, 5, 6, 0, 0 })));
                // HK RK table: "vhk = :vhk AND vrk > :vrk (flavor 1: length(vrk+vhk) < max)"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " > :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[] { 4, 5, 6, 0 },
                        ":value3", getBytesFilledWithMax(new byte[0]))));
                // HK RK table: "vhk = :vhk AND vrk > :vrk (flavor 2: length(vrk+vhk) = max)"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " > :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(getBytesFilledWithMax(new byte[] { 4, 5, 6 }))),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[] { 4, 5, 7 },
                        ":value3", getBytesFilledWithMax(new byte[0]))));
                // HK RK table: "vhk = :vhk AND vrk >= :vrk"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex, virtualRkField + " >= :vrk",
                    ImmutableMap.of(":vrk", getVirtualRkValue(new byte[] { 4, 5, 6 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[] { 4, 5, 6 },
                        ":value3", getBytesFilledWithMax(new byte[0]))));
                // HK RK table: "vhk = :vhk AND vrk BETWEEN :vrk1 AND :vrk2"
                inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                    virtualRkField + " BETWEEN :vrk1 AND :vrk2",
                    ImmutableMap.of(":vrk1", getVirtualRkValue(new byte[] { 4 }),
                        ":vrk2", getVirtualRkValue(new byte[] { 5 })),
                    "#field2 BETWEEN :value2 AND :value3",
                    getPhysicalRkValueMap(":value2", new byte[] { 4 }, ":value3", new byte[] { 5 })));
            }
            return inputs.build();
        }

        private static AttributeValue getVirtualRkValue(byte[] rkRawValue) {
            return new AttributeValue().withB(ByteBuffer.wrap(rkRawValue));
        }

        private static AttributeValue getPhysicalRkValueFromVirtualRk(byte[] rkRawValue) {
            return getPhysicalRkValue(HK_TYPE, VIRTUAL_HK_VALUE.getAttributeValue(), RK_TYPE,
                new AttributeValue().withB(ByteBuffer.wrap(rkRawValue)));
        }

        private static byte[] getBytesFilledWithMax(byte[] prefix) {
            byte[] byteArray = new byte[MAX_VIRTUAL_RK_LENGTH];
            for (int i = 0; i < byteArray.length; i++) {
                byteArray[i] = i < prefix.length ? prefix[i] : UnsignedBytes.MAX_VALUE;
            }
            return byteArray;
        }

        private static ImmutableMap<String, AttributeValue> getPhysicalRkValueMap(String placeholder1,
                                                                                  byte[] rkRawValue1,
                                                                                  String placeholder2,
                                                                                  byte[] rkRawValue2) {
            return ImmutableMap.of(placeholder1, getPhysicalRkValueFromVirtualRk(rkRawValue1),
                placeholder2, getPhysicalRkValueFromVirtualRk(rkRawValue2));
        }
    }

    @Test
    void testApplyToKeyConditionParsing() {
        // field place holders, extra spaces, "AND" and "BETWEEN" not all uppercase
        String expression = "#vhk   =:vhk And  #vrk  beTWEEn :vrk1 aNd :vrk2";
        // "#field1" is already taken
        Map<String, String> fields = ImmutableMap.of("#vhk", VIRTUAL_HK, "#vrk", VIRTUAL_RK,
            "#field1", SOME_FIELD);
        // ":value1" is already taken
        Map<String, AttributeValue> values = ImmutableMap.of(
            ":vhk", KeyConditionTestInputs.VIRTUAL_HK_VALUE.getAttributeValue(),
            ":vrk1", KeyConditionTestInputs.getVirtualRkValue(new byte[] { 1, 2, 3 }),
            ":vrk2", KeyConditionTestInputs.getVirtualRkValue(new byte[] { 4, 5 }),
            ":value1", new AttributeValue("someValue"));

        String expectedExpression = "#field2 = :value2 AND #field3 BETWEEN :value3 AND :value4";
        Map<String, String> expectedFields = ImmutableMap.of("#field1", SOME_FIELD, "#field2", PHYSICAL_HK,
            "#field3", PHYSICAL_RK);
        Map<String, AttributeValue> expectedValues = ImmutableMap.<String, AttributeValue>builder()
            .put(":value1", new AttributeValue("someValue"))
            .put(":value2", getPhysicalHkValue(KeyConditionTestInputs.VIRTUAL_HK_VALUE))
            .putAll(KeyConditionTestInputs.getPhysicalRkValueMap(
                ":value3", new byte[] { 1, 2, 3 }, ":value4", new byte[] { 4, 5 }))
            .build();

        runApplyToKeyConditionTest(KeyConditionTestInputs.VIRTUAL_HK_RK_TABLE, false /*useSecondaryIndex*/,
            expression, fields, values, expectedExpression, expectedFields, expectedValues);
    }

}
