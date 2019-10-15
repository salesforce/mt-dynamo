/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedBytes;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.AbstractQueryAndScanMapper.QueryRequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.TestHashKeyValue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashPartitioningConditionMapperTest {

    private static final ScalarAttributeType HK_TYPE = B;
    private static final ScalarAttributeType RK_TYPE = B;
    private static final DynamoTableDescription VIRTUAL_HK_TABLE = buildVirtualHkTable(HK_TYPE);
    private static final DynamoTableDescription VIRTUAL_HK_RK_TABLE =
        buildVirtualHkRkTable(HK_TYPE, RK_TYPE, HK_TYPE, RK_TYPE);
    private static final byte[] VIRTUAL_HK_BYTES = new byte[] {1, 2, 3};
    private static final TestHashKeyValue VIRTUAL_HK_VALUE = new TestHashKeyValue(VIRTUAL_HK_BYTES);
    private static final int MAX_VIRTUAL_RK_LENGTH = 1024 - 2 - VIRTUAL_HK_BYTES.length;

    @ParameterizedTest(name = "{index}")
    @MethodSource("buildTestInputs")
    void testApplyToKeyCondition(DynamoTableDescription virtualTable, boolean useSecondaryIndex,
                                 @Nullable String rkExpression, Map<String, AttributeValue> rkValues,
                                 String expectedRkExpression, Map<String, AttributeValue> expectedRkValues) {
        String expression = (useSecondaryIndex ? VIRTUAL_GSI_HK : VIRTUAL_HK) + " = :vhk";
        if (rkExpression != null) {
            expression += " AND " + rkExpression;
        }
        Map<String, AttributeValue> values = new HashMap<>(rkValues);
        values.put(":vhk", VIRTUAL_HK_VALUE.getAttributeValue());

        String expectedExpression = "#field1 = :value1 AND " + expectedRkExpression;
        Map<String, String> expectedFields = ImmutableMap.of(
            "#field1", useSecondaryIndex ? PHYSICAL_GSI_HK : PHYSICAL_HK,
            "#field2", useSecondaryIndex ? PHYSICAL_GSI_RK : PHYSICAL_RK);
        Map<String, AttributeValue> expectedValues = new HashMap<>(expectedRkValues);
        expectedValues.put(":value1", getPhysicalHkValue(VIRTUAL_HK_VALUE));

        runApplyToKeyConditionTest(virtualTable, useSecondaryIndex, expression, null /*fields*/, values,
            expectedExpression, expectedFields, expectedValues);
    }

    @Test
    void testApplyToKeyConditionParsing() {
        // field place holders, extra spaces, "AND" and "BETWEEN" not all uppercase
        String expression = "#vhk   = :vhk And  #vrk  beTWEEn :vrk1 aNd :vrk2";
        // "#field1" is already taken
        Map<String, String> fields = ImmutableMap.of("#vhk", VIRTUAL_HK, "#vrk", VIRTUAL_RK,
            "#field1", SOME_FIELD);
        // ":value1" is already taken
        Map<String, AttributeValue> values = ImmutableMap.of(":vhk", VIRTUAL_HK_VALUE.getAttributeValue(),
            ":vrk1", getVirtualRkValue(new byte[]{1, 2, 3}),
            ":vrk2", getVirtualRkValue(new byte[]{4, 5}),
            ":value1", new AttributeValue("someValue"));

        String expectedExpression = "#field2 = :value2 AND #field3 BETWEEN :value3 AND :value4";
        Map<String, String> expectedFields = ImmutableMap.of("#field1", SOME_FIELD, "#field2", PHYSICAL_HK,
            "#field3", PHYSICAL_RK);
        Map<String, AttributeValue> expectedValues = ImmutableMap.<String, AttributeValue>builder()
            .put(":value1", new AttributeValue("someValue"))
            .put(":value2", getPhysicalHkValue(VIRTUAL_HK_VALUE))
            .putAll(getPhysicalRkValueMap(":value3", new byte[]{1, 2, 3}, ":value4", new byte[]{4, 5}))
            .build();

        runApplyToKeyConditionTest(VIRTUAL_HK_RK_TABLE, false /*useSecondaryIndex*/, expression, fields, values,
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
        mapper.applyToKeyCondition(requestWrapper, requestIndex);

        assertEquals(expectedExpression, request.getKeyConditionExpression());
        assertEquals(expectedFields, request.getExpressionAttributeNames());
        assertEquals(expectedValues, request.getExpressionAttributeValues());
    }

    private static Stream<Arguments> buildTestInputs() {
        List<Arguments> inputs = new LinkedList<>();
        for (Boolean useSecondaryIndex : ImmutableList.of(Boolean.FALSE, Boolean.TRUE)) {
            String virtualRkField = useSecondaryIndex ? VIRTUAL_GSI_RK : VIRTUAL_RK;
            // HK table: "vhk = :vhk"
            inputs.add(Arguments.of(VIRTUAL_HK_TABLE, useSecondaryIndex,
                null, Collections.emptyMap(),
                "#field2 = :value2",
                ImmutableMap.of(":value2", getPhysicalRkValue(HK_TYPE, VIRTUAL_HK_VALUE.getAttributeValue()))));
            // HK RK table: "vhk = :vhk AND vrk = :vrk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " = :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[]{4, 5, 6})),
                "#field2 = :value2",
                ImmutableMap.of(":value2", getPhysicalRkValueFromVirtualRk(new byte[]{4, 5, 6}))));
            // HK RK table; "vhk = :vhk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                null, Collections.emptyMap(),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[0],
                    ":value3", getBytesFilledWithMax(new byte[0]))));
            // HK RK table: "vhk = :vhk AND vrk < :vrk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " < :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[]{4, 5, 6, 0, 0})),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[0],
                    ":value3", getBytesFilledWithMax(new byte[]{4, 5, 5, -1, -1}))));
            // HK RK table: "vhk = :vhk AND vrk <= :vrk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " <= :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[]{4, 5, 6, 0, 0})),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[0],
                    ":value3", new byte[]{4, 5, 6, 0, 0})));
            // HK RK table: "vhk = :vhk AND vrk > :vrk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " > :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[]{4, 5, 6})),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[]{4, 5, 6, 0},
                    ":value3", getBytesFilledWithMax(new byte[0]))));
            // HK RK table: "vhk = :vhk AND vrk >= :vrk"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " >= :vrk", ImmutableMap.of(":vrk", getVirtualRkValue(new byte[]{4, 5, 6})),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[]{4, 5, 6},
                    ":value3", getBytesFilledWithMax(new byte[0]))));
            // HK RK table: "vhk = :vhk AND vrk BETWEEN :vrk1 AND :vrk2"
            inputs.add(Arguments.of(VIRTUAL_HK_RK_TABLE, useSecondaryIndex,
                virtualRkField + " BETWEEN :vrk1 AND :vrk2",
                ImmutableMap.of(":vrk1", getVirtualRkValue(new byte[]{4}), ":vrk2", getVirtualRkValue(new byte[]{5})),
                "#field2 BETWEEN :value2 AND :value3",
                getPhysicalRkValueMap(":value2", new byte[]{4}, ":value3", new byte[]{5})));
        }
        return inputs.stream();
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
