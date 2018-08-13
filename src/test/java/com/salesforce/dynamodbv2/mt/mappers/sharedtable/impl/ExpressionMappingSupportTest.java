package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.ExpressionMappingSupport.RequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryMapper.QueryRequestWrapper;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests ExpressionMappingSupport.
 *
 * @author msgroi
 */
class ExpressionMappingSupportTest {

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
        ExpressionMappingSupport.convertFieldNameLiteralsToExpressionNames(fieldMappings, requestWrapper);
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
        ExpressionMappingSupport.convertFieldNameLiteralsToExpressionNames(fieldMappings, requestWrapper);
        assertEquals("#field1 = :value and field2 = :value2 and #field1 = :value3",
            requestWrapper.getPrimaryExpression());
        assertEquals(ImmutableMap.of("#field1", "field"), requestWrapper.getExpressionAttributeNames());
    }

}