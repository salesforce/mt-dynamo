package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARYINDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.QueryAndScanMapper.QueryRequestWrapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

    @ParameterizedTest(name = "{index}")
    @MethodSource("applyKeyConditionToFieldInvocations")
    void applyKeyConditionToField(KeyConditionTestInvocation testInvocation) {
        executeKeyConditionsTest(testInvocation);
    }

    private void executeKeyConditionsTest(KeyConditionTestInvocation testInvocation) {
        final KeyConditionTestInputs inputs = testInvocation.getInputs();
        final KeyConditionTestExpected expected = testInvocation.getExpected();
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();
        mtContext.setContext(inputs.getOrg());
        DynamoTableDescription virtualTable = mock(DynamoTableDescription.class);
        when(virtualTable.getTableName()).thenReturn(inputs.getVirtualTableName());
        when(tableMapping.getVirtualTable()).thenReturn(virtualTable);
        ConditionMapper sut = new ConditionMapper(tableMapping,
                new FieldMapper(mtContext, inputs.getVirtualTableName(), new FieldPrefixFunction(".")));
        RequestWrapper requestWrapper = inputs.getRequestWrapper();
        sut.applyKeyConditionToField(requestWrapper,
                inputs.getFieldMapping(),
                inputs.getPrimaryExpression(),
                inputs.getFilterExpression());
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
        private String filterExpression;

        private KeyConditionTestInputs() {
        }

        private KeyConditionTestInputs(String org,
                                       String virtualTableName,
                                       String[] attributeNames,
                                       String[] attributeValues,
                                       FieldMapping fieldMapping,
                                       String primaryExpression,
                                       String filterExpression) {
            this.org = org;
            this.virtualTableName = virtualTableName;
            this.attributeNames = attributeNames;
            this.attributeValues = attributeValues;
            this.fieldMapping = fieldMapping;
            this.primaryExpression = primaryExpression;
            this.filterExpression = filterExpression;
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

        KeyConditionTestInputs filterExpression(String filterExpression) {
            this.filterExpression = filterExpression;
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
            when(requestWrapper.getExpressionAttributeNames()).thenReturn(toAttributeNames(attributeNames));
            when(requestWrapper.getExpressionAttributeValues()).thenReturn(toAttributeValues(attributeValues));
            return requestWrapper;
        }

        FieldMapping getFieldMapping() {
            return fieldMapping;
        }

        String getPrimaryExpression() {
            return primaryExpression;
        }

        String getFilterExpression() {
            return filterExpression;
        }

        public KeyConditionTestInputs build() {
            return new KeyConditionTestInputs(org,
                    virtualTableName,
                    attributeNames,
                    attributeValues,
                    fieldMapping,
                    primaryExpression,
                    filterExpression);
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
        private KeyConditionTestInputs inputs;
        private KeyConditionTestExpected expected;

        KeyConditionTestInvocation(KeyConditionTestInputs inputs, KeyConditionTestExpected expected) {
            this.inputs = inputs;
            this.expected = expected;
        }

        KeyConditionTestInputs getInputs() {
            return inputs;
        }

        public KeyConditionTestExpected getExpected() {
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
                                .attributeNames("#field1", "virtualhk")
                                .attributeValues(":value", "hkvalue")
                                .fieldMapping(new FieldMapping(new Field("virtualhk", S), new Field("physicalhk", S),
                                        "virtualTable",
                                        "null",
                                        TABLE,
                                        true))
                                .primaryExpression("#field1 = :value")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#field1", "physicalhk")
                                .attributeValues(":value", "ctx.virtualTable.hkvalue").build()
                ),
                // map gsi hash-key field name and value on a primary expression on a table with hk only
                new KeyConditionTestInvocation(
                        new KeyConditionTestInputs()
                                .org("ctx")
                                .virtualTableName("virtualTable")
                                .attributeNames("#field", "virtualgsihk")
                                .attributeValues(":value", "hkgsivalue")
                                .fieldMapping(new FieldMapping(
                                        new Field("virtualgsihk", S), new Field("physicalgsihk", S),
                                        "virtualgsi",
                                        "physicalgsi",
                                        SECONDARYINDEX,
                                        true))
                                .primaryExpression("#field = :value")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#field", "physicalgsihk")
                                .attributeValues(":value", "ctx.virtualgsi.hkgsivalue").build()
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
                                        "mt_sharedtablestatic_s_s",
                                        TABLE,
                                        true))
                                .primaryExpression("#name = :value AND #name2 = :value2")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name", "hk")
                                .attributeValues(":value", "ctx1.Table3.1").build()
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
                                        "mt_sharedtablestatic_s_s",
                                        TABLE,
                                        false))
                                .primaryExpression("#name = :value AND #name2 = :value2")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name2", "rk")
                                .attributeValues(":value2", "rangeKeyValue").build()
                ),
                // map table's hash-key field name on a filter expression on a table with hk and rk
                new KeyConditionTestInvocation(
                        new KeyConditionTestInputs()
                                .org("ctx1")
                                .virtualTableName("Table3")
                                .attributeNames("#name2", "someField", "#name", "hashKeyField")
                                .attributeValues(":value2", "someValue3a", ":value", "hashKeyValue3")
                                .fieldMapping(new FieldMapping(
                                        new Field("hashKeyField", S), new Field("hk", S),
                                        "Table3",
                                        "mt_sharedtablestatic_s_s",
                                        TABLE,
                                        true))
                                .primaryExpression("#name = :value")
                                .filterExpression("#name2 = :value2").build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name", "hk")
                                .attributeValues(":value", "ctx1.Table3.hashKeyValue3").build()
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
                                        "mt_sharedtablestatic_s_s",
                                        TABLE,
                                        false))
                                .primaryExpression("set #someField = :newValue")
                                .filterExpression("#hk = :currentHkValue and #rk = :currentRkValue").build(),
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
                                        "testgsi",
                                        "gsi_s",
                                        SECONDARYINDEX,
                                        true))
                                .primaryExpression("#name = :value")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name", "gsi_s_hk")
                                .attributeValues(":value", "ctx1.testgsi.indexFieldValue").build()
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
                                        "testlsi",
                                        "lsi_s_s",
                                        TABLE,
                                        true))
                                .primaryExpression("#name = :value and #name2 = :value2")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name", "hk")
                                .attributeValues(":value", "ctx1.Table3.1").build()
                ),
                // map lsi range-key field name on a primary expression on a table with hk and rk
                new KeyConditionTestInvocation(
                        new KeyConditionTestInputs()
                                .org("ctx1")
                                .virtualTableName("Table3")
                                .attributeNames("#name", "hk", "#name2", "indexField")
                                .attributeValues(":value", "ctx1.Table3.1", ":value2", "indexFieldValue")
                                .fieldMapping(new FieldMapping(
                                        new Field("indexField", S), new Field("lsi_s_s_rk", S),
                                        "testlsi",
                                        "lsi_s_s",
                                        SECONDARYINDEX,
                                        false))
                                .primaryExpression("#name = :value and #name2 = :value2")
                                .filterExpression(null).build(),
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
                                        "testgsi",
                                        "gsi_s",
                                        SECONDARYINDEX,
                                        true))
                                .primaryExpression("#name = :value and begins_with(#___name___, :___value___)")
                                .filterExpression(null).build(),
                        new KeyConditionTestExpected()
                                .attributeNames("#name", "gsi_s_hk")
                                .attributeValues(":value", "ctx1.testgsi.indexFieldValue").build()
                ),
                // map attribute_exists expression
                new KeyConditionTestInvocation(
                    new KeyConditionTestInputs()
                        .org("ctx")
                        .virtualTableName("virtualTable")
                        .attributeNames("#field1", "virtualhk")
                        .fieldMapping(new FieldMapping(new Field("virtualhk", S), new Field("physicalhk", S),
                            "virtualTable",
                            "null",
                            TABLE,
                            true))
                        .primaryExpression("attribute_exists(#field1)")
                        .filterExpression(null).build(),
                    new KeyConditionTestExpected()
                        .attributeNames("#field1", "physicalhk").build()
                )
        ).stream();
    }

    private static Map<String, String> toAttributeNames(String... attributeNames) {
        return IntStream.range(0, attributeNames.length / 2).map(i -> i * 2)
                .collect(HashMap::new, (m,i) -> m.put(attributeNames[i], attributeNames[i + 1]), Map::putAll);
    }

    private static Map<String, AttributeValue> toAttributeValues(String... attributeValues) {
        return attributeValues == null
            ? new HashMap<>()
            : IntStream.range(0, attributeValues.length / 2).map(i -> i * 2)
                .collect(HashMap::new, (m,i) -> m.put(attributeValues[i],
                        new AttributeValue().withS(attributeValues[i + 1])), Map::putAll);
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
            "#hk = :currentValue", "#hk").get());
        assertEquals(":currentHkValue", ConditionMapper.findVirtualValuePlaceholder(
                "set #someField = :newValue",
                "#hk = :currentHkValue and #rk = :currentRkValue",
                "#hk").get());
        assertEquals(":currentRkValue", ConditionMapper.findVirtualValuePlaceholder(
                "set #someField = :newValue",
                "#hk = :currentHkValue and #rk = :currentRkValue",
                "#rk").get());
        assertFalse(ConditionMapper.findVirtualValuePlaceholder(
                "set #someField = :newValue",
                "#hk = :currentValue", "#invalid").isPresent());
        assertFalse(ConditionMapper.findVirtualValuePlaceholder(
            null, null, "#invalid").isPresent());
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
        assertEquals(Optional.empty(), ConditionMapper.findVirtualValuePlaceholder(
            null, null));
    }

}