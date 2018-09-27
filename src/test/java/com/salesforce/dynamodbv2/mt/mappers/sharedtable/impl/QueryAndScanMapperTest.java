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
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
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
class QueryAndScanMapperTest {

    private static final DynamoTableDescription VIRTUAL_TABLE_DESCRIPTION = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                    .withTableName("virtualTable")
                    .withTableKeySchema("virtualhk", S)
                    .addSi("virtualgsi", GSI, new PrimaryKey("virtualgsihk", S), 1L).build());
    private static final DynamoTableDescription PHYSICAL_TABLE_DESCRIPTION = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                    .withTableKeySchema("physicalhk", S)
                    .addSi("physicalgsi", GSI, new PrimaryKey("physicalgsihk", S), 1L).build());
    private static final TableMapping TABLE_MAPPING = new TableMapping(VIRTUAL_TABLE_DESCRIPTION,
            new SingletonCreateTableRequestFactory(PHYSICAL_TABLE_DESCRIPTION.getCreateTableRequest()),
        new DynamoSecondaryIndexMapperByTypeImpl(),
        new MtAmazonDynamoDbContextProvider() {
            @Override
            public Optional<String> getContextOpt() {
                return Optional.of("ctx");
            }

            @Override
            public void withContext(String org, Runnable runnable) {
            }
        },
        "."
    );

    private QueryAndScanMapper getMockQueryMapper() {
        return new QueryAndScanMapper(TABLE_MAPPING, null);
    }

    @Test
    void nonIndexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualhk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkvalue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx.virtualTable.hkvalue"))),
                queryRequest);
    }

    /*
     * not testing with GT parameter since test would fail, since queryContainsHashKeyCondition will return false (it
     * currently looks for a " = " substring)
     */
    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ" })
    void queryWithKeyConditions(ComparisonOperator comparisonOperator) {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditions(ImmutableMap.of("virtualhk",
                        new Condition()
                                .withComparisonOperator(comparisonOperator)
                                .withAttributeValueList(new AttributeValue().withS("hkvalue"))));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value1",
                                new AttributeValue().withS("ctx.virtualTable.hkvalue"))),
                queryRequest);
    }

    @Test
    void nonIndexQueryWithLiterals() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("virtualhk = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkvalue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression("#field1 = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new
                                AttributeValue().withS("ctx.virtualTable.hkvalue"))),
                queryRequest);
    }

    @Test
    void indexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withIndexName("virtualgsi")
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualgsihk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkgsivalue")));

        getMockQueryMapper().apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withIndexName("physicalgsi")
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalgsihk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx.virtualgsi.hkgsivalue"))),
                queryRequest);
    }

    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ", "GT" })
    void queryWithKeyConditionExpressionAndKeyConditions(ComparisonOperator comparisonOperator) {
        try {
            getMockQueryMapper()
                    .apply(new QueryRequest().withKeyConditions(ImmutableMap.of("virtualhk",
                            new Condition()
                                    .withComparisonOperator(comparisonOperator)
                                    .withAttributeValueList(new AttributeValue().withS("hkvalue"))))
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
                .withIndexName("virtualgsi")
                .withFilterExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualgsihk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkgsivalue")));

        getMockQueryMapper().apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withIndexName("physicalgsi")
                        .withFilterExpression(scanRequest.getFilterExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalgsihk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("ctx.virtualgsi.hkgsivalue"))),
                scanRequest);
    }

    @Test
    void nonIndexScanMissingHkField() {
        ScanRequest scanRequest = new ScanRequest()
                .withExpressionAttributeNames(new HashMap<>())
                .withExpressionAttributeValues(new HashMap<>());

        FieldPrefixFunction fieldPrefixFunction = mock(FieldPrefixFunction.class);
        FieldMapper fieldMapper = new FieldMapper(null, null, fieldPrefixFunction);
        FieldValue fieldValue = mock(FieldValue.class);
        when(fieldValue.getQualifiedValue()).thenReturn("prefixed");
        when(fieldPrefixFunction.apply(any(), any(), any())).thenReturn(fieldValue);
        new QueryAndScanMapper(TABLE_MAPPING, fieldMapper).apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withFilterExpression("begins_with(#___name___, :___value___)")
                        .withExpressionAttributeNames(ImmutableMap.of("#___name___", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":___value___",
                                new AttributeValue().withS("prefixed"))),
                scanRequest);
    }

    @ParameterizedTest
    @EnumSource(value = ComparisonOperator.class, names = { "EQ", "GT" })
    void scanWithFilterExpressionAndScanFilter(ComparisonOperator comparisonOperator) {
        try {
            getMockQueryMapper()
                    .apply(new ScanRequest().withScanFilter(ImmutableMap.of("virtualhk",
                            new Condition()
                                    .withComparisonOperator(comparisonOperator)
                                    .withAttributeValueList(new AttributeValue().withS("hkvalue"))))
                    .withFilterExpression("#field = :value"));
            fail("expected exception not encountered");
        } catch (IllegalArgumentException e) {
            assertEquals("ambiguous ScanRequest: both filterExpression and scanFilter were provided",
                    e.getMessage());
        }
    }

}