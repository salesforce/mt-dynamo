/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class RandomPartitioningQueryAndScanMapperTest {

    private static final DynamoTableDescription VIRTUAL_TABLE = new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder.builder()
            .withTableName("virtualTable")
            .withTableKeySchema("virtualHk", S)
            .addSi("virtualGsi", GSI, new PrimaryKey("virtualGsiHk", S), 1L).build());
    private static final DynamoTableDescription PHYSICAL_TABLE = new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder.builder()
            .withTableKeySchema("physicalHk", S)
            .addSi("physicalGsi", GSI, new PrimaryKey("physicalGsiHk", S), 1L).build());
    private static final RandomPartitioningTableMapping TABLE_MAPPING = new RandomPartitioningTableMapping(
        VIRTUAL_TABLE,
        PHYSICAL_TABLE,
        index -> index.equals(VIRTUAL_TABLE.findSi("virtualGsi")) ? PHYSICAL_TABLE.findSi("physicalGsi") : null,
        () -> Optional.of("ctx")
    );

    private RandomPartitioningQueryAndScanMapper getMockQueryMapper() {
        return new RandomPartitioningQueryAndScanMapper(TABLE_MAPPING, null);
    }

    @Test
    void nonIndexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualHk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkValue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx/virtualTable/hkValue"))),
                queryRequest);
    }

    /*
     * not testing with GT, GE, LT, or LE parameters since test would fail, since queryContainsHashKeyCondition will
     * return false (it currently looks for a " = " substring)
     */
    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ" })
    void queryWithKeyConditions(ComparisonOperator comparisonOperator) {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditions(ImmutableMap.of("virtualHk",
                        new Condition()
                                .withComparisonOperator(comparisonOperator)
                                .withAttributeValueList(new AttributeValue().withS("hkValue"))));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value1",
                                new AttributeValue().withS("ctx/virtualTable/hkValue"))),
                queryRequest);
    }

    @Test
    void nonIndexQueryWithLiterals() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("virtualHk = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkValue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression("#field1 = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new
                                AttributeValue().withS("ctx/virtualTable/hkValue"))),
                queryRequest);
    }

    @Test
    void indexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withIndexName("virtualGsi")
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualGsiHk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkGsiValue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withIndexName("physicalGsi")
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalGsiHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx/virtualTable/hkGsiValue"))),
                queryRequest);
    }

    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ", "GT", "GE", "LT", "LE" })
    void queryWithKeyConditionExpressionAndKeyConditions(ComparisonOperator comparisonOperator) {
        try {
            getMockQueryMapper()
                    .apply(new QueryRequest().withKeyConditions(ImmutableMap.of("virtualHk",
                            new Condition()
                                    .withComparisonOperator(comparisonOperator)
                                    .withAttributeValueList(new AttributeValue().withS("hkValue"))))
                            .withKeyConditionExpression("#field = :value"));
            fail("expected exception not encountered");
        } catch (IllegalArgumentException e) {
            assertEquals("ambiguous QueryRequest: both keyConditionExpression and keyConditions were provided",
                    e.getMessage());
        }
    }

    @Test
    void queryWithNeitherKeyConditionExpressionNorKeyConditions() {
        try {
            getMockQueryMapper().apply(new QueryRequest());
            fail("expected exception not encountered");
        } catch (IllegalArgumentException e) {
            assertEquals("keyConditionExpression or keyConditions are required", e.getMessage());
        }
    }

    @Test
    void indexScan() {
        ScanRequest scanRequest = new ScanRequest()
                .withIndexName("virtualGsi")
                .withFilterExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualGsiHk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkGsiValue")));

        getMockQueryMapper().apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withIndexName("physicalGsi")
                        .withFilterExpression(scanRequest.getFilterExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalGsiHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx/virtualTable/hkGsiValue"))),
                scanRequest);
    }

    @Test
    void nonIndexScanMissingHkField() {
        ScanRequest scanRequest = new ScanRequest()
                .withExpressionAttributeNames(new HashMap<>())
                .withExpressionAttributeValues(new HashMap<>());

        FieldMapper fieldMapper = mock(FieldMapper.class);
        when(fieldMapper.apply(any(), any())).thenReturn(new AttributeValue("prefixed"));
        new RandomPartitioningQueryAndScanMapper(TABLE_MAPPING, fieldMapper).apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withFilterExpression("begins_with(#___name___, :___value___)")
                        .withExpressionAttributeNames(ImmutableMap.of("#___name___", "physicalHk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":___value___",
                                new AttributeValue().withS("prefixed"))),
                scanRequest);
    }

    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ", "GT", "GE", "LT", "LE" })
    void scanWithFilterExpressionAndScanFilter(ComparisonOperator comparisonOperator) {
        try {
            getMockQueryMapper()
                    .apply(new ScanRequest().withScanFilter(ImmutableMap.of("virtualHk",
                            new Condition()
                                    .withComparisonOperator(comparisonOperator)
                                    .withAttributeValueList(new AttributeValue().withS("hkValue"))))
                    .withFilterExpression("#field = :value"));
            fail("expected exception not encountered");
        } catch (IllegalArgumentException e) {
            assertEquals("ambiguous ScanRequest: both filterExpression and scanFilter were provided",
                    e.getMessage());
        }
    }

}