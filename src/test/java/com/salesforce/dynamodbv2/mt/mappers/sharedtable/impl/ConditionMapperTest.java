package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryMapper.QueryRequestWrapper;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests ConditionMapper.
 *
 * @author msgroi
 */
class ConditionMapperTest {

    private static ConditionMapper SUT = null;

    @BeforeAll
    static void beforeAll() {
        TableMapping tableMapping = mock(TableMapping.class);
        when(tableMapping.getAllVirtualToPhysicalFieldMappingsDeduped())
            .thenReturn(ImmutableMap.of("field", new FieldMapping(new Field("field", S),
            new Field("field", S),
            null,
            null,
            TABLE,
            true)));
        SUT = new ConditionMapper(tableMapping, null);
    }

    @Test
    void convertFieldNameLiteralsToExpressionNames() {
        RequestWrapper requestWrapper = new QueryRequestWrapper(new QueryRequest()
            .withKeyConditionExpression("field = :value")
            .withExpressionAttributeNames(new HashMap<>())
            .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue())));
        SUT.convertFieldNameLiteralsToExpressionNames(requestWrapper);
        assertEquals("#field1 = :value", requestWrapper.getPrimaryExpression());
        assertEquals(ImmutableMap.of("#field1", "field"), requestWrapper.getExpressionAttributeNames());
    }

    @Test
    void convertFieldNameLiteralsToExpressionNamesMultiple() {
        RequestWrapper requestWrapper = new QueryRequestWrapper(new QueryRequest()
            .withKeyConditionExpression("field = :value and field2 = :value2 and field = :value3")
            .withExpressionAttributeNames(new HashMap<>())
            .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue())));
        SUT.convertFieldNameLiteralsToExpressionNames(requestWrapper);
        assertEquals("#field1 = :value and field2 = :value2 and #field1 = :value3",
            requestWrapper.getPrimaryExpression());
        assertEquals(ImmutableMap.of("#field1", "field"), requestWrapper.getExpressionAttributeNames());
    }

}