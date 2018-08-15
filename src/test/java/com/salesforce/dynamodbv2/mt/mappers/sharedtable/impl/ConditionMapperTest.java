package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryMapper.QueryRequestWrapper;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests ConditionMapper.
 *
 * @author msgroi
 */
class ConditionMapperTest {

    private static TableMapping tableMapping = mock(TableMapping.class);
    private static ConditionMapper SUT = null;

    @BeforeAll
    static void beforeAll() {
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

    @Test
    void getNextFieldPlaceholder() {
        assertEquals("#field0", ConditionMapper.getNextFieldPlaceholder(
                new HashMap<>(), new AtomicInteger(0)));
        assertEquals("#field1", ConditionMapper.getNextFieldPlaceholder(
                new HashMap<>(), new AtomicInteger(1)));
        assertEquals("#field2", ConditionMapper.getNextFieldPlaceholder(
                ImmutableMap.of("#field0", "somename0", "#field1", "somename1"),
                new AtomicInteger(0)));
    }

    @Test
    void findVirtualValuePlaceholderInEitherExpression() {
        assertEquals(":currentValue", ConditionMapper.findVirtualValuePlaceholder(
            "set #someField = :newValue",
            "#hk = :currentValue", "#hk"));
        assertEquals(":currentHkValue", ConditionMapper.findVirtualValuePlaceholder(
                "set #someField = :newValue",
                "#hk = :currentHkValue and #rk = :currentRkValue",
                "#hk"));
        assertEquals(":currentRkValue", ConditionMapper.findVirtualValuePlaceholder(
                "set #someField = :newValue",
                "#hk = :currentHkValue and #rk = :currentRkValue",
                "#rk"));
        try {
            ConditionMapper.findVirtualValuePlaceholder(
                    "set #someField = :newValue",
                    "#hk = :currentValue", "#invalid");
            fail("expected IllegalArgumentException not encountered");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().startsWith("field #invalid not found"));
        }
    }

    @Test
    void findVirtualValuePlaceholder() {
        assertEquals(Optional.empty(), ConditionMapper.findVirtualValuePlaceholder(
            "set #someField = :newValue", "#hk"));
        assertEquals(":currentValue", ConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentValue", "#hk").get());
        assertEquals(":currentHkValue", ConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk = :currentRkValue", "#hk").get());
        assertEquals(":currentRkValue", ConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk = :currentRkValue", "#rk").get());
    }

}