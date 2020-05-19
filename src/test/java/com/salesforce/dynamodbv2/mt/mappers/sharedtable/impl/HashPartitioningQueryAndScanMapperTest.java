/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.NUM_BUCKETS;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_TABLE_NAME;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkRkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getKeyMapper;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getPhysicalRkValue;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getTableMapping;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

class HashPartitioningQueryAndScanMapperTest {

    private static final ScalarAttributeType HK_TYPE = B;
    private static final ScalarAttributeType RK_TYPE = B;
    private static final DynamoTableDescription VIRTUAL_HK_TABLE = buildVirtualHkTable(HK_TYPE);
    private static final DynamoTableDescription VIRTUAL_HK_RK_TABLE =
        buildVirtualHkRkTable(HK_TYPE, RK_TYPE, HK_TYPE, RK_TYPE);

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanHkTable(boolean onGsi) {
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = new HashMap<>();
        List<AttributeValue> virtualHkValues2 = getVirtualHkValuesForBucket(2 /*bucket*/, 2 /*count*/);
        List<AttributeValue> virtualHkValues5 = getVirtualHkValuesForBucket(5 /*bucket*/, 1 /*count*/);
        physicalRkValuesPerBucket.put(2, getPhysicalRkValues(virtualHkValues2));
        physicalRkValuesPerBucket.put(5, getPhysicalRkValues(virtualHkValues5));

        List<Map<String, AttributeValue>> expectedItems = getExpectedItems(onGsi, physicalRkValuesPerBucket.get(2));

        runScanTest(VIRTUAL_HK_TABLE, onGsi, physicalRkValuesPerBucket, null /*exclusiveStartKey*/,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanHkTableWithStartKey(boolean onGsi) {
        List<AttributeValue> virtualHkValues2 = getVirtualHkValuesForBucket(2 /*bucket*/, 2 /*count*/);
        List<AttributeValue> virtualHkValues5 = getVirtualHkValuesForBucket(5 /*bucket*/, 1 /*count*/);
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = ImmutableMap.of(
            2, getPhysicalRkValues(virtualHkValues2),
            5, getPhysicalRkValues(virtualHkValues5));

        // scan with exclusive start key in the middle of bucket 2 -- should return rest of bucket 2
        Map<String, AttributeValue> exclusiveStartKey = getExclusiveStartKey(onGsi, virtualHkValues2.get(0));
        List<Map<String, AttributeValue>> expectedItems = getExpectedItems(onGsi,
            physicalRkValuesPerBucket.get(2).subList(1, 2));
        runScanTest(VIRTUAL_HK_TABLE, onGsi, physicalRkValuesPerBucket, exclusiveStartKey,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);

        // scan with exclusive start key at the end of bucket 2 -- should return next non-empty bucket
        exclusiveStartKey = getExclusiveStartKey(onGsi, virtualHkValues2.get(1));
        expectedItems = getExpectedItems(onGsi, physicalRkValuesPerBucket.get(5));
        runScanTest(VIRTUAL_HK_TABLE, onGsi, physicalRkValuesPerBucket, exclusiveStartKey,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanHkRkTable(boolean onGsi) {
        AttributeValue virtualHkValue2 = getVirtualHkValueForBucket(2);
        AttributeValue virtualHkValue5 = getVirtualHkValueForBucket(5);
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = ImmutableMap.of(
            2, getPhysicalRkValues(virtualHkValue2, new byte[] { 1, 2 }, new byte[] { 3 }),
            5, getPhysicalRkValues(virtualHkValue5, new byte[] { 4 }));

        List<Map<String, AttributeValue>> expectedItems = getExpectedItems(onGsi, virtualHkValue2,
            new byte[] { 1, 2 }, new byte[] { 3 });

        runScanTest(VIRTUAL_HK_RK_TABLE, onGsi, physicalRkValuesPerBucket, null /*exclusiveStartKey*/,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanHkRkTableWithStartKey(boolean onGsi) {
        AttributeValue virtualHkValue2 = getVirtualHkValueForBucket(2);
        AttributeValue virtualHkValue5 = getVirtualHkValueForBucket(5);
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = ImmutableMap.of(
            2, getPhysicalRkValues(virtualHkValue2, new byte[] { 1, 2 }, new byte[] { 3 }),
            5, getPhysicalRkValues(virtualHkValue5, new byte[] { 4 }));

        // scan with an exclusive start key in the middle of bucket 2 -- should return rest of bucket 2
        Map<String, AttributeValue> exclusiveStartKey = getExclusiveStartKey(onGsi,
            virtualHkValue2,
            new byte[] { 1, 2 });
        List<Map<String, AttributeValue>> expectedItems = getExpectedItems(onGsi, virtualHkValue2, new byte[] { 3 });
        runScanTest(VIRTUAL_HK_RK_TABLE, onGsi, physicalRkValuesPerBucket, exclusiveStartKey,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);

        // scan with exclusive start key at the end of bucket 2 -- should return next non-empty bucket
        exclusiveStartKey = getExclusiveStartKey(onGsi, virtualHkValue2, new byte[] { 3 });
        expectedItems = getExpectedItems(onGsi, virtualHkValue5, new byte[] { 4 });
        runScanTest(VIRTUAL_HK_RK_TABLE, onGsi, physicalRkValuesPerBucket, exclusiveStartKey,
            expectedItems, true /*shouldHaveLastEvaluatedKey*/);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanNoRecords(boolean onGsi) {
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = Collections.emptyMap();
        List<Map<String, AttributeValue>> expectedItems = Collections.emptyList();
        runScanTest(VIRTUAL_HK_RK_TABLE, onGsi, physicalRkValuesPerBucket, null /*exclusiveStartKey*/,
            expectedItems, false /*shouldHaveLastEvaluatedKey*/);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void testScanLastBucket(boolean onGsi) {
        int lastBucket = NUM_BUCKETS - 1;
        AttributeValue virtualHkValue = getVirtualHkValueForBucket(lastBucket);
        Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket = ImmutableMap.of(
            lastBucket, getPhysicalRkValues(virtualHkValue, new byte[] { 1, 2 }, new byte[] { 3 }));

        List<Map<String, AttributeValue>> expectedItems = getExpectedItems(onGsi, virtualHkValue,
            new byte[] { 1, 2 }, new byte[] { 3 });

        // lastEvaluatedKey should be set to null if we got to the end of the last bucket
        runScanTest(VIRTUAL_HK_RK_TABLE, onGsi, physicalRkValuesPerBucket, null /*exclusiveStartKey*/,
            expectedItems, false /*shouldHaveLastEvaluatedKey*/);
    }

    private void runScanTest(DynamoTableDescription virtualTable, boolean onGsi,
                             Map<Integer, List<AttributeValue>> physicalRkValuesPerBucket,
                             @Nullable Map<String, AttributeValue> exclusiveStartKey,
                             List<Map<String, AttributeValue>> expectedItems,
                             boolean shouldHaveLastEvaluatedKey) {
        if (shouldHaveLastEvaluatedKey) {
            assertFalse(expectedItems.isEmpty());
        }

        AmazonDynamoDB amazonDynamoDb = mock(AmazonDynamoDB.class);
        when(amazonDynamoDb.query(any())).thenAnswer(new PhysicalQueryAnswer(onGsi, physicalRkValuesPerBucket));

        HashPartitioningQueryAndScanMapper mapper = getTableMapping(virtualTable).getQueryAndScanMapper();
        ScanRequest request = new ScanRequest()
            .withTableName(VIRTUAL_TABLE_NAME)
            .withExclusiveStartKey(exclusiveStartKey);
        if (onGsi) {
            request.withIndexName(VIRTUAL_GSI);
        }
        ScanResult result = mapper.executeScan(amazonDynamoDb, request);

        assertEquals(expectedItems, result.getItems());
        assertEquals(shouldHaveLastEvaluatedKey ? expectedItems.get(expectedItems.size() - 1) : null,
            result.getLastEvaluatedKey());
    }

    private List<Map<String, AttributeValue>> getExpectedItems(boolean onGsi, List<AttributeValue> virtualHkValues) {
        String physicalHk = onGsi ? PHYSICAL_GSI_HK : PHYSICAL_HK;
        String physicalRk = onGsi ? PHYSICAL_GSI_RK : PHYSICAL_RK;
        HashPartitioningKeyMapper keyMapper = getKeyMapper(VIRTUAL_HK_TABLE);
        return virtualHkValues.stream()
            .map(vhk -> ImmutableMap.of(physicalHk, keyMapper.toPhysicalHashKey(HK_TYPE, vhk),
                physicalRk, getPhysicalRkValue(HK_TYPE, vhk)))
            .collect(Collectors.toList());
    }

    private List<Map<String, AttributeValue>> getExpectedItems(boolean onGsi, AttributeValue virtualHkValue,
                                                               byte[]... virtualRkRawValues) {
        String physicalHk = onGsi ? PHYSICAL_GSI_HK : PHYSICAL_HK;
        String physicalRk = onGsi ? PHYSICAL_GSI_RK : PHYSICAL_RK;
        AttributeValue physicalHkValue = getKeyMapper(VIRTUAL_HK_RK_TABLE).toPhysicalHashKey(HK_TYPE, virtualHkValue);
        return getPhysicalRkValues(virtualHkValue, virtualRkRawValues).stream()
            .map(prk -> ImmutableMap.of(physicalHk, physicalHkValue, physicalRk, prk))
            .collect(Collectors.toList());
    }

    private Map<String, AttributeValue> getExclusiveStartKey(boolean onGsi, AttributeValue virtualHkValue) {
        Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();
        exclusiveStartKey.put(onGsi ? VIRTUAL_GSI_HK : VIRTUAL_HK, virtualHkValue);
        if (onGsi) {
            exclusiveStartKey.put(VIRTUAL_HK, toAttributeValue(new byte[] { 0 }));
        }
        return exclusiveStartKey;
    }

    private Map<String, AttributeValue> getExclusiveStartKey(boolean onGsi, AttributeValue virtualHkValue,
                                                             byte[] virtualRkRawValue) {
        Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();
        exclusiveStartKey.put(onGsi ? VIRTUAL_GSI_HK : VIRTUAL_HK, virtualHkValue);
        exclusiveStartKey.put(onGsi ? VIRTUAL_GSI_RK : VIRTUAL_RK, toAttributeValue(virtualRkRawValue));
        if (onGsi) {
            exclusiveStartKey.put(VIRTUAL_HK, toAttributeValue(new byte[] { 0 }));
            exclusiveStartKey.put(VIRTUAL_RK, toAttributeValue(new byte[] { 0 }));
        }
        return exclusiveStartKey;
    }

    private List<AttributeValue> getPhysicalRkValues(List<AttributeValue> virtualHkValues) {
        return virtualHkValues.stream().map(vhk -> getPhysicalRkValue(HK_TYPE, vhk)).collect(Collectors.toList());
    }

    private List<AttributeValue> getPhysicalRkValues(AttributeValue virtualHkValue, byte[]... virtualRkRawValues) {
        return Stream.of(virtualRkRawValues)
            .map(rawVrk -> getPhysicalRkValue(HK_TYPE, virtualHkValue, RK_TYPE, toAttributeValue(rawVrk)))
            .collect(Collectors.toList());
    }

    private AttributeValue getVirtualHkValueForBucket(int bucket) {
        return getVirtualHkValuesForBucket(bucket, 1).get(0);
    }

    private List<AttributeValue> getVirtualHkValuesForBucket(int bucket, int numValues) {
        List<AttributeValue> result = new ArrayList<>(numValues);
        for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE && result.size() < numValues; i++) {
            byte[] byteArray = new byte[] { i };
            if (Arrays.hashCode(byteArray) % NUM_BUCKETS == bucket) {
                result.add(toAttributeValue(byteArray));
            }
        }
        if (result.size() < numValues) {
            throw new RuntimeException("Unable to find enough virtual HK values for bucket " + bucket);
        }
        return result;
    }

    private AttributeValue toAttributeValue(byte[] byteArray) {
        return new AttributeValue().withB(ByteBuffer.wrap(byteArray));
    }

    private static class PhysicalQueryAnswer implements Answer<QueryResult> {

        private final String physicalHk;
        private final String physicalRk;
        private final Map<Integer, List<AttributeValue>> rkValuesPerBucket;

        PhysicalQueryAnswer(boolean onGsi, Map<Integer, List<AttributeValue>> rkValuesPerBucket) {
            this.physicalHk = onGsi ? PHYSICAL_GSI_HK : PHYSICAL_HK;
            this.physicalRk = onGsi ? PHYSICAL_GSI_RK : PHYSICAL_RK;
            this.rkValuesPerBucket = rkValuesPerBucket;
        }

        @Override
        public QueryResult answer(InvocationOnMock invocationOnMock) {
            QueryRequest queryRequest = invocationOnMock.getArgument(0);
            assertEquals("#field1 = :value1", queryRequest.getKeyConditionExpression());
            assertEquals(physicalHk, queryRequest.getExpressionAttributeNames().get("#field1"));
            AttributeValue physicalHkValue = queryRequest.getExpressionAttributeValues().get(":value1");
            int bucket = getBucketFromPhysicalHkValue(physicalHkValue);

            List<Map<String, AttributeValue>> items = new LinkedList<>();
            List<AttributeValue> rkValuesInBucket = rkValuesPerBucket.get(bucket);

            Optional<AttributeValue> startKeyRk = Optional.empty();
            if (queryRequest.getExclusiveStartKey() != null) {
                assertEquals(physicalHkValue, queryRequest.getExclusiveStartKey().get(physicalHk));
                startKeyRk = Optional.of(queryRequest.getExclusiveStartKey().get(physicalRk));
            }

            if (rkValuesInBucket != null && !rkValuesInBucket.isEmpty()) {
                boolean canStart = startKeyRk.isEmpty();
                for (AttributeValue rk : rkValuesInBucket) {
                    if (canStart || (canStart = (comparePhysicalRkValues(rk, startKeyRk.get()) > 0))) {
                        items.add(ImmutableMap.of(physicalHk, physicalHkValue, physicalRk, rk));
                    }
                }
            }

            return new QueryResult().withItems(items).withCount(items.size());
        }

        private int getBucketFromPhysicalHkValue(AttributeValue value) {
            ByteBuffer byteBuffer = value.getB();
            return byteBuffer.getInt(byteBuffer.limit() - 4);
        }

        private int comparePhysicalRkValues(AttributeValue v1, AttributeValue v2) {
            return Arrays.compare(v1.getB().array(), v2.getB().array());
        }
    }
}
