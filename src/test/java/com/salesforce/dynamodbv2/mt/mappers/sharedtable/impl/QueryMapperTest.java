/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryMapper.QueryRequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryMapper.RequestWrapper;

import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/*
 * @author msgroi
 */
class QueryMapperTest {

    private static final DynamoTableDescription VIRTUAL_TABLE_DESCRIPTION = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                    .withTableKeySchema("virtualhk", S)
                    .addSi("virtualgsi", GSI, new PrimaryKey("virtualgsihk", S), 1L).build());
    private static final DynamoTableDescription PHYSICAL_TABLE_DESCRIPTION = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                    .withTableKeySchema("physicalhk", S)
                    .addSi("physicalgsi", GSI, new PrimaryKey("physicalgsihk", S), 1L).build());
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    private static final TableMapping TABLE_MAPPING = new TableMapping(VIRTUAL_TABLE_DESCRIPTION,
            virtualTableDescription1 -> PHYSICAL_TABLE_DESCRIPTION.getCreateTableRequest(),
            new DynamoSecondaryIndexMapperByTypeImpl(),
            () -> "ctx",
            "."
    );

    private QueryMapper getMockQueryMapper(String fieldMapperReturnValue) {
        FieldMapper fieldMapper = mock(FieldMapper.class);
        when(fieldMapper.apply(any(), any())).thenReturn(new AttributeValue().withS(fieldMapperReturnValue));
        return new QueryMapper(TABLE_MAPPING, fieldMapper);
    }

    @Test
    void nonIndexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualhk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkvalue")));

        getMockQueryMapper("prefixed-hkvalue").apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("prefixed-hkvalue"))),
                queryRequest);
    }

    @Test
    void queryWithKeyConditions() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditions(ImmutableMap.of("virtualhk",
                        new Condition()
                                .withComparisonOperator(EQ)
                                .withAttributeValueList(new AttributeValue().withS("hkvalue"))));

        getMockQueryMapper("prefixed-hkvalue").apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value1",
                                new AttributeValue().withS("prefixed-hkvalue"))),
                queryRequest);
    }

    @Test
    void nonIndexQueryWithLiterals() {
        QueryRequest queryRequest = new QueryRequest()
                .withKeyConditionExpression("virtualhk = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkvalue")));

        getMockQueryMapper("prefixed-hkvalue").apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withKeyConditionExpression("#field1 = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#field1", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new
                                AttributeValue().withS("prefixed-hkvalue"))),
                queryRequest);
    }

    @Test
    void indexQuery() {
        QueryRequest queryRequest = new QueryRequest()
                .withIndexName("virtualgsi")
                .withKeyConditionExpression("#field = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "virtualgsihk"))
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hkgsivalue")));

        getMockQueryMapper("prefixed-hkgsivalue").apply(queryRequest);

        assertEquals(new QueryRequest()
                        .withIndexName("physicalgsi")
                        .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalgsihk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("prefixed-hkgsivalue"))),
                queryRequest);
    }

    @Test
    void queryWithKeyConditionExpressionAndKeyConditions() {
        try {
            getMockQueryMapper(null)
                    .apply(new QueryRequest().withKeyConditions(ImmutableMap.of("virtualhk",
                            new Condition()
                                    .withComparisonOperator(EQ)
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
            getMockQueryMapper(null).apply(new QueryRequest());
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

        getMockQueryMapper("prefixed-hkgsivalue").apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withIndexName("physicalgsi")
                        .withFilterExpression(scanRequest.getFilterExpression())
                        .withExpressionAttributeNames(ImmutableMap.of("#field", "physicalgsihk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                                new AttributeValue().withS("prefixed-hkgsivalue"))),
                scanRequest);
    }

    @Test
    void nonIndexScanMissingHkField() {
        ScanRequest scanRequest = new ScanRequest()
                .withExpressionAttributeNames(new HashMap<>())
                .withExpressionAttributeValues(new HashMap<>());

        getMockQueryMapper("prefixed").apply(scanRequest);

        assertEquals(new ScanRequest()
                        .withFilterExpression("begins_with(#___name___, :___value___)")
                        .withExpressionAttributeNames(ImmutableMap.of("#___name___", "physicalhk"))
                        .withExpressionAttributeValues(ImmutableMap.of(":___value___",
                                new AttributeValue().withS("prefixed"))),
                scanRequest);
    }

    @Test
    void scanWithFilterExpressionAndScanFilter() {
        try {
            getMockQueryMapper(null)
                    .apply(new ScanRequest().withScanFilter(ImmutableMap.of("virtualhk",
                            new Condition()
                                    .withComparisonOperator(EQ)
                                    .withAttributeValueList(new AttributeValue().withS("hkvalue"))))
                    .withFilterExpression("#field = :value"));
            fail("expected exception not encountered");
        } catch (IllegalArgumentException e) {
            assertEquals("ambiguous ScanRequest: both filterExpression and scanFilter were provided",
                    e.getMessage());
        }
    }

    @Test
    void convertFieldNameLiteralsToExpressionNames() {
        List<FieldMapping> fieldMappings = ImmutableList.of(new FieldMapping(new Field("field", S),
                new Field("field", S),
                null,
                null,
                TABLE,
                true));
        RequestWrapper requestWrapper = new QueryRequestWrapper(new QueryRequest()
                .withKeyConditionExpression("field = :value")
                .withExpressionAttributeNames(new HashMap<>())
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue())));
        getMockQueryMapper(null)
                .convertFieldNameLiteralsToExpressionNames(fieldMappings, requestWrapper);
        assertEquals("#field1 = :value", requestWrapper.getPrimaryExpression());
        assertEquals(ImmutableMap.of("#field1", "field"), requestWrapper.getExpressionAttributeNames());
    }

    @Test
    void convertFieldNameLiteralsToExpressionNamesMultiple() {
        List<FieldMapping> fieldMappings = ImmutableList.of(new FieldMapping(
                new Field("field", S),
                new Field("field", S),
                null,
                null,
                TABLE,
                true));
        RequestWrapper requestWrapper = new QueryRequestWrapper(new QueryRequest()
                .withKeyConditionExpression("field = :value and field2 = :value2 and field = :value3")
                .withExpressionAttributeNames(new HashMap<>())
                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue())));
        getMockQueryMapper(null)
                .convertFieldNameLiteralsToExpressionNames(fieldMappings, requestWrapper);
        assertEquals("#field1 = :value and field2 = :value2 and #field1 = :value3",
                requestWrapper.getPrimaryExpression());
        assertEquals(ImmutableMap.of("#field1", "field"), requestWrapper.getExpressionAttributeNames());
    }

}