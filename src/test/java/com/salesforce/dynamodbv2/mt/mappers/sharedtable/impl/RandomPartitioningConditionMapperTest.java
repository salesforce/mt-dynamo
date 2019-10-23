package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARY_INDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.AbstractQueryAndScanMapper.QueryRequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RandomPartitioningConditionMapper.UpdateConditionExpressionRequestWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests ConditionMapper.
 *
 * @author msgroi
 */
class RandomPartitioningConditionMapperTest {

    private static final RandomPartitioningTableMapping tableMapping = mock(RandomPartitioningTableMapping.class);

    @ParameterizedTest(name = "{index}")
    @MethodSource("applyKeyConditionToFieldInvocations")
    void applyKeyConditionToField(KeyConditionTestInvocation testInvocation) {
        executeKeyConditionsTest(testInvocation);
    }

    private void executeKeyConditionsTest(KeyConditionTestInvocation testInvocation) {
        final KeyConditionTestInputs inputs = testInvocation.getInputs();
        final KeyConditionTestExpected expected = testInvocation.getExpected();
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        mtContext.setContext(inputs.getOrg());
        DynamoTableDescription virtualTable = mock(DynamoTableDescription.class);
        when(virtualTable.getTableName()).thenReturn(inputs.getVirtualTableName());
        when(tableMapping.getVirtualTable()).thenReturn(virtualTable);
        RandomPartitioningConditionMapper sut = new RandomPartitioningConditionMapper(tableMapping,
            new StringFieldMapper(mtContext, inputs.getVirtualTableName()));
        RequestWrapper requestWrapper = inputs.getRequestWrapper();
        sut.mapFieldInConditionExpression(requestWrapper, inputs.getFieldMapping());
        expected.getAttributeNames().forEach((name, value) ->
            verify(requestWrapper).putExpressionAttributeName(name, value));
        expected.getAttributeValues().forEach((name, attributeValue) ->
            verify(requestWrapper).putExpressionAttributeValue(name, attributeValue));
    }

    private static class KeyConditionTestInputs {
        private String org;
        private String virtualTableName;
        private String[] attributeNames;
        private String[] attributeValues;
        private FieldMapping fieldMapping;
        private String primaryExpression;

        private KeyConditionTestInputs() {
        }

        private KeyConditionTestInputs(String org,
                                       String virtualTableName,
                                       String[] attributeNames,
                                       String[] attributeValues,
                                       FieldMapping fieldMapping,
                                       String primaryExpression) {
            this.org = org;
            this.virtualTableName = virtualTableName;
            this.attributeNames = attributeNames;
            this.attributeValues = attributeValues;
            this.fieldMapping = fieldMapping;
            this.primaryExpression = primaryExpression;
        }

        KeyConditionTestInputs org(String org) {
            this.org = org;
            return this;
        }


        KeyConditionTestInputs virtualTableName(String virtualTableName) {
            this.virtualTableName = virtualTableName;
            return this;
        }

        KeyConditionTestInputs attributeNames(String... attributeNames) {
            this.attributeNames = attributeNames;
            return this;
        }

        KeyConditionTestInputs attributeValues(String... attributeValues) {
            this.attributeValues = attributeValues;
            return this;
        }

        KeyConditionTestInputs fieldMapping(FieldMapping fieldMapping) {
            this.fieldMapping = fieldMapping;
            return this;
        }

        KeyConditionTestInputs primaryExpression(String primaryExpression) {
            this.primaryExpression = primaryExpression;
            return this;
        }

        String getOrg() {
            return org;
        }

        String getVirtualTableName() {
            return virtualTableName;
        }

        RequestWrapper getRequestWrapper() {
            RequestWrapper requestWrapper = mock(RequestWrapper.class);
            when(requestWrapper.getExpression()).thenReturn(primaryExpression);
            when(requestWrapper.getExpressionAttributeNames()).thenReturn(toAttributeNames(attributeNames));
            when(requestWrapper.getExpressionAttributeValues()).thenReturn(toAttributeValues(attributeValues));
            return requestWrapper;
        }

        FieldMapping getFieldMapping() {
            return fieldMapping;
        }

        KeyConditionTestInputs build() {
            return new KeyConditionTestInputs(org,
                virtualTableName,
                attributeNames,
                attributeValues,
                fieldMapping,
                primaryExpression);
        }

    }

    private static class KeyConditionTestExpected {
        private String[] attributeNames;
        private String[] attributeValues;

        KeyConditionTestExpected() {
        }

        private KeyConditionTestExpected(String[] attributeNames, String[] attributeValues) {
            this.attributeNames = attributeNames;
            this.attributeValues = attributeValues;
        }

        KeyConditionTestExpected attributeNames(String... attributeNames) {
            this.attributeNames = attributeNames;
            return this;
        }

        KeyConditionTestExpected attributeValues(String... attributeValues) {
            this.attributeValues = attributeValues;
            return this;
        }

        Map<String, String> getAttributeNames() {
            return toAttributeNames(attributeNames);
        }

        Map<String, AttributeValue> getAttributeValues() {
            return toAttributeValues(attributeValues);
        }

        KeyConditionTestExpected build() {
            return new KeyConditionTestExpected(attributeNames, attributeValues);
        }
    }

    private static class KeyConditionTestInvocation {
        private final KeyConditionTestInputs inputs;
        private final KeyConditionTestExpected expected;

        KeyConditionTestInvocation(KeyConditionTestInputs inputs, KeyConditionTestExpected expected) {
            this.inputs = inputs;
            this.expected = expected;
        }

        KeyConditionTestInputs getInputs() {
            return inputs;
        }

        KeyConditionTestExpected getExpected() {
            return expected;
        }
    }

    private static Stream<KeyConditionTestInvocation> applyKeyConditionToFieldInvocations() {
        return ImmutableList.of(
            // map table's hash-key field name field and value on a primary expression on a table with hk only
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx")
                    .virtualTableName("virtualTable")
                    .attributeNames("#field1", "virtualHk")
                    .attributeValues(":value", "hkValue")
                    .fieldMapping(new FieldMapping(new Field("virtualHk", S), new Field("physicalHk", S),
                        "virtualTable",
                        "null",
                        TABLE,
                        true))
                    .primaryExpression("#field1 = :value")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#field1", "physicalHk")
                    .attributeValues(":value", "ctx/virtualTable/hkValue").build()
            ),
            // map gsi hash-key field name and value on a primary expression on a table with hk only
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx")
                    .virtualTableName("virtualTable")
                    .attributeNames("#field", "virtualGsiHk")
                    .attributeValues(":value", "hkGsiValue")
                    .fieldMapping(new FieldMapping(
                        new Field("virtualGsiHk", S), new Field("physicalGsiHk", S),
                        "virtualGsi",
                        "physicalGsi",
                        SECONDARY_INDEX,
                        true))
                    .primaryExpression("#field = :value")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#field", "physicalGsiHk")
                    .attributeValues(":value", "ctx/virtualTable/hkGsiValue").build()
            ),
            // map table's hash-key field name and value on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#name2", "rk", "#name", "hashKeyField")
                    .attributeValues(":value2", "rangeKeyValue", ":value", "1")
                    .fieldMapping(new FieldMapping(new Field("hashKeyField", S), new Field("hk", S),
                        "Table3",
                        "mt_shared_table_static_s_s",
                        TABLE,
                        true))
                    .primaryExpression("#name = :value AND #name2 = :value2")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name", "hk")
                    .attributeValues(":value", "ctx1/Table3/1").build()
            ),
            // map table's range-key field name on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#name2", "rangeKeyField", "#name", "hashKeyField")
                    .attributeValues(":value2", "rangeKeyValue", ":value", "1")
                    .fieldMapping(new FieldMapping(new Field("rangeKeyField", S), new Field("rk", S),
                        "Table3",
                        "mt_shared_table_static_s_s",
                        TABLE,
                        false))
                    .primaryExpression("#name = :value AND #name2 = :value2")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name2", "rk")
                    .attributeValues(":value2", "rangeKeyValue").build()
            ),
            // map table's range-key field name on a filter expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("Org-51")
                    .virtualTableName("Table3")
                    .attributeNames("#someField", "someField", "#hk", "hashKeyField", "#rk",
                        "rangeKeyField")
                    .attributeValues(":currentRkValue", "rangeKeyValue", ":currentHkValue", "1",
                        ":newValue", "someValueTable3Org-51Updated")
                    .fieldMapping(new FieldMapping(new Field("rangeKeyField", S), new Field("rk", S),
                        "Table3",
                        "mt_shared_table_static_s_s",
                        TABLE,
                        false))
                    .primaryExpression("#hk = :currentHkValue and #rk = :currentRkValue")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#rk", "rk")
                    .attributeValues(":currentRkValue", "rangeKeyValue").build()
            ),
            // map gsi hash-key field name and value on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#name", "indexField")
                    .attributeValues(":value", "indexFieldValue")
                    .fieldMapping(new FieldMapping(new Field("indexField", S), new Field("gsi_s_hk", S),
                        "testGsi",
                        "gsi_s",
                        SECONDARY_INDEX,
                        true))
                    .primaryExpression("#name = :value")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name", "gsi_s_hk")
                    .attributeValues(":value", "ctx1/Table3/indexFieldValue").build()
            ),
            // map lsi hash-key field name and value on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#name", "hashKeyField", "#name2", "indexField")
                    .attributeValues(":value", "1", ":value2", "indexFieldValue")
                    .fieldMapping(new FieldMapping(
                        new Field("hashKeyField", S), new Field("hk", S),
                        "testLsi",
                        "lsi_s_s",
                        TABLE,
                        true))
                    .primaryExpression("#name = :value and #name2 = :value2")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name", "hk")
                    .attributeValues(":value", "ctx1/Table3/1").build()
            ),
            // map lsi range-key field name on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#name", "hk", "#name2", "indexField")
                    .attributeValues(":value", "ctx1/Table3/1", ":value2", "indexFieldValue")
                    .fieldMapping(new FieldMapping(
                        new Field("indexField", S), new Field("lsi_s_s_rk", S),
                        "testLsi",
                        "lsi_s_s",
                        SECONDARY_INDEX,
                        false))
                    .primaryExpression("#name = :value and #name2 = :value2")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name2", "lsi_s_s_rk")
                    .attributeValues(":value2", "indexFieldValue").build()
            ),
            // map gsi hash-key field name and value on a primary expression on a table with hk and rk
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx1")
                    .virtualTableName("Table3")
                    .attributeNames("#___name___", "hk", "#name", "indexField")
                    .attributeValues(":___value___", "ctx1.Table3.", ":value", "indexFieldValue")
                    .fieldMapping(new FieldMapping(new Field("indexField", S), new Field("gsi_s_hk", S),
                        "testGsi",
                        "gsi_s",
                        SECONDARY_INDEX,
                        true))
                    .primaryExpression("#name = :value and begins_with(#___name___, :___value___)")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#name", "gsi_s_hk")
                    .attributeValues(":value", "ctx1/Table3/indexFieldValue").build()
            ),
            // map attribute_exists expression
            new KeyConditionTestInvocation(
                new KeyConditionTestInputs()
                    .org("ctx")
                    .virtualTableName("virtualTable")
                    .attributeNames("#field1", "virtualHk")
                    .fieldMapping(new FieldMapping(new Field("virtualHk", S), new Field("physicalHk", S),
                        "virtualTable",
                        "null",
                        TABLE,
                        true))
                    .primaryExpression("attribute_exists(#field1)")
                    .build(),
                new KeyConditionTestExpected()
                    .attributeNames("#field1", "physicalHk").build()
            )
        ).stream();
    }

    private static Map<String, String> toAttributeNames(String... attributeNames) {
        return IntStream.range(0, attributeNames.length / 2).map(i -> i * 2)
            .collect(HashMap::new, (m, i) -> m.put(attributeNames[i], attributeNames[i + 1]), Map::putAll);
    }

    private static Map<String, AttributeValue> toAttributeValues(String... attributeValues) {
        return attributeValues == null
            ? new HashMap<>()
            : IntStream.range(0, attributeValues.length / 2).map(i -> i * 2)
            .collect(HashMap::new, (m, i) -> m.put(attributeValues[i],
                new AttributeValue().withS(attributeValues[i + 1])), Map::putAll);
    }

    @Test
    void convertFieldNameLiteralsToExpressionNames() {
        String expression = "literal = :value AND attribute_exists(literal) AND prefix_literal_suffix > :value2"
            + " AND #literal = :value3";
        String expected = "#field1 = :value AND attribute_exists(#field1) AND #field2 > :value2"
            + " AND #literal = :value3";
        QueryRequest request = new QueryRequest()
            .withFilterExpression(expression)
            .withExpressionAttributeNames(new HashMap<>())
            .withExpressionAttributeValues(Collections.emptyMap() /*doesn't matter*/);
        RequestWrapper requestWrapper = new QueryRequestWrapper(request, request::getFilterExpression,
            request::setFilterExpression);
        RandomPartitioningConditionMapper.convertFieldNameLiteralsToExpressionNames(
            requestWrapper, ImmutableList.of("literal", "other_literal", "prefix_literal_suffix"));
        assertEquals(expected, request.getFilterExpression());
        assertEquals(ImmutableMap.of("#field1", "literal", "#field2", "prefix_literal_suffix"),
            request.getExpressionAttributeNames());
    }

    @Test
    void convertFieldNameLiteralsToExpressionNames_allSupportedOperators() {
        validateConvertFieldNameLiteralsSingleLiteral("literal = :value");
        validateConvertFieldNameLiteralsSingleLiteral("literal <> :value");
        validateConvertFieldNameLiteralsSingleLiteral("literal > :value");
        validateConvertFieldNameLiteralsSingleLiteral("literal >= :value");
        validateConvertFieldNameLiteralsSingleLiteral("literal < :value");
        validateConvertFieldNameLiteralsSingleLiteral("literal <= :value");
        validateConvertFieldNameLiteralsSingleLiteral("attribute_exists(literal)");
        validateConvertFieldNameLiteralsSingleLiteral("attribute_not_exists(literal)");
    }

    private void validateConvertFieldNameLiteralsSingleLiteral(String expression) {
        QueryRequest request = new QueryRequest()
            .withFilterExpression(expression)
            .withExpressionAttributeNames(new HashMap<>())
            .withExpressionAttributeValues(Collections.emptyMap() /*doesn't matter*/);
        RequestWrapper requestWrapper = new QueryRequestWrapper(request, request::getFilterExpression,
            request::setFilterExpression);
        RandomPartitioningConditionMapper.convertFieldNameLiteralsToExpressionNames(
            requestWrapper, ImmutableList.of("literal"));
        assertEquals(expression.replace("literal", "#field1"), request.getFilterExpression());
        assertEquals(ImmutableMap.of("#field1", "literal"), request.getExpressionAttributeNames());
    }

    @Test
    void getNextPlaceholder() {
        assertEquals("#field1", MappingUtils.getNextPlaceholder(
            new HashMap<>(), "#field"));
        assertEquals("#field2", MappingUtils.getNextPlaceholder(
            ImmutableMap.of("#field1", "literal"), "#field"));
        assertEquals(":value3", MappingUtils.getNextPlaceholder(
            ImmutableMap.of(":value1", "someValue1", ":value2", "someValue2"), ":value"));
    }

    @Test
    void findVirtualValuePlaceholder() {
        assertEquals(Optional.empty(), RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "set #someField = :newValue", "#hk"));
        assertEquals(":currentValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentValue", "#hk").orElseThrow());
        assertEquals(":currentHkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk = :currentRkValue", "#hk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk = :currentRkValue", "#rk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk <> :currentRkValue", "#rk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk > :currentRkValue", "#rk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk >= :currentRkValue", "#rk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk < :currentRkValue", "#rk").orElseThrow());
        assertEquals(":currentRkValue", RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "#hk = :currentHkValue and #rk <= :currentRkValue", "#rk").orElseThrow());
        assertEquals(Optional.of(":ue1"), RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            "set #ue1 = :ue1, #ue2 = :ue2", "#ue1"));
        assertEquals(Optional.empty(), RandomPartitioningConditionMapper.findVirtualValuePlaceholder(
            null, null));
    }

    @Test
    void makePlaceholdersDistinct() {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            .withUpdateExpression("SET #field1 = :value1, #field3 = :value2")
            .withConditionExpression("#field1 = :value1 AND #field2 <> :value2 AND #field3 > :value1")
            .withExpressionAttributeNames(ImmutableMap.of("#field1", "A", "#field2", "B", "#field3", "C"))
            .withExpressionAttributeValues(ImmutableMap.of(":value1", new AttributeValue("x"),
                ":value2", new AttributeValue("y")));
        RandomPartitioningConditionMapper.makePlaceholdersDistinct(updateItemRequest.getUpdateExpression(),
            new UpdateConditionExpressionRequestWrapper(updateItemRequest));

        assertEquals("#field5 = :value4 AND #field2 <> :value3 AND #field4 > :value4",
            updateItemRequest.getConditionExpression());
        assertEquals(ImmutableMap.of("#field1", "A",
            "#field2", "B",
            "#field3", "C",
            "#field4", "C",
            "#field5", "A"),
            updateItemRequest.getExpressionAttributeNames());
        assertEquals(ImmutableMap.of(":value1", new AttributeValue("x"),
            ":value2", new AttributeValue("y"),
            ":value3", new AttributeValue("y"),
            ":value4", new AttributeValue("x")),
            updateItemRequest.getExpressionAttributeValues());
    }

    /* TODO HK: There's a bug if a field is in both the update and condition expressions, and it's in multiple secondary
       indexes. when it's fixed, the commented out parts below should be uncommented. */
    @Test
    void applyToUpdate() {
        Map<String, List<FieldMapping>> allMappingsPerField = ImmutableMap.of("field1", ImmutableList.of(
            new FieldMapping(new Field("field1", S), new Field("physicalGsi1Hk", S), null, null, null, true),
            new FieldMapping(new Field("field1", S), new Field("physicalGsi2Rk", S), null, null, null, false)
        ));
        RandomPartitioningTableMapping tableMapping = mock(RandomPartitioningTableMapping.class);
        when(tableMapping.getAllMappingsPerField()).thenReturn(allMappingsPerField);
        DynamoTableDescription virtualTable = mock(DynamoTableDescription.class);
        when(virtualTable.getPrimaryKey()).thenReturn(new PrimaryKey("hk", S));
        when(tableMapping.getVirtualTable()).thenReturn(virtualTable);
        RandomPartitioningConditionMapper mapper = new RandomPartitioningConditionMapper(tableMapping,
            new StringFieldMapper(() -> Optional.of("ctx"), "virtualTable"));

        UpdateItemRequest request = new UpdateItemRequest()
            .withUpdateExpression("SET #field1 = :value1, #field2 = :value2")
            //.withConditionExpression("#field1 = :oldValue")
            .withExpressionAttributeNames(ImmutableMap.of("#field1", "field1", "#field2", "field2"))
            .withExpressionAttributeValues(ImmutableMap.of(":value1", new AttributeValue("x"),
                ":value2", new AttributeValue("y")/*, ":oldValue", new AttributeValue("y")*/));
        mapper.applyForUpdate(request);

        assertEquals("SET #field1 = :value1, #field3 = :value3, #field2 = :value2", request.getUpdateExpression());
        //assertEquals("#field4 = :oldValue", request.getConditionExpression());
        assertEquals(ImmutableMap.of("#field1", "physicalGsi1Hk",
            "#field2", "field2",
            "#field3", "physicalGsi2Rk"/*,
            "#field4", "field1"*/),
            request.getExpressionAttributeNames());
        assertEquals(ImmutableMap.of(":value1", new AttributeValue("ctx/virtualTable/x"),
            ":value2", new AttributeValue("y"),
            ":value3", new AttributeValue("x")/*,
            ":oldValue", new AttributeValue("ctx/virtualTable/z")*/),
            request.getExpressionAttributeValues());
    }

}