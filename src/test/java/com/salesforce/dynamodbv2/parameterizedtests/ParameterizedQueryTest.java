package com.salesforce.dynamodbv2.parameterizedtests;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createHkAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ParameterizedTestArgumentProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests query().
 *
 * @author msgroi
 */
@Tag("parameterized-tests")
class ParameterizedQueryTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void query(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            String keyConditionExpression = "#name = :value";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#name", HASH_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap
                .of(":value", createHkAttribute(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE1)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                SOME_FIELD_VALUE + TABLE1 + org)));
            assertEquals(TABLE1, queryRequest.getTableName()); // assert no side effects
            assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression)); // assert no side effects
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryWithKeyConditions(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb()
                .query(new QueryRequest().withTableName(TABLE1)
                    .withKeyConditions(ImmutableMap.of(
                        HASH_KEY_FIELD,
                        new Condition().withComparisonOperator(EQ).withAttributeValueList(createHkAttribute(
                            testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))))).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryUsingAttributeNamePlaceholders(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE1).withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createHkAttribute(
                        testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)))).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    // Note: field names with '-' will fail if you use literals instead of expressionAttributeNames()
    void queryUsingAttributeNameLiterals(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            List<Map<String, AttributeValue>> items =
                testArgument.getAmazonDynamoDb().query(new QueryRequest().withTableName(TABLE1)
                .withKeyConditionExpression(HASH_KEY_FIELD + " = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createHkAttribute(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)))).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            String keyConditionExpression = "#hk = :hkv and #rk = :rkv";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of(
                "#hk", HASH_KEY_FIELD,
                "#rk", RANGE_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap.of(
                ":hkv", createHkAttribute(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                ":rkv", createStringAttribute(RANGE_KEY_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildHkRkItemWithSomeFieldValue(
                testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org)));
            assertEquals(TABLE3, queryRequest.getTableName()); // assert no side effects
            assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression)); // assert no side effects
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryHkRkTableNoRkSpecified(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            String keyConditionExpression = "#hk = :hkv";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#hk", HASH_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues =
                ImmutableMap.of(":hkv", createHkAttribute(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(2, items.size());
            assertThat(items.get(0), is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                SOME_FIELD_VALUE + TABLE3 + org)));
            assertThat(items.get(1), is(buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE,
                Optional.of(RANGE_KEY_VALUE + "2"),
                SOME_FIELD_VALUE + TABLE3 + org + "2",
                Optional.of(INDEX_FIELD_VALUE))));
            assertEquals(TABLE3, queryRequest.getTableName()); // assert no side effects
            assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression)); // assert no side effects
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
            // assert no side effects
            assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryHkRkWithFilterExpression(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3).withKeyConditionExpression("#name = :value")
                    .withFilterExpression("#name2 = :value2")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", SOME_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createHkAttribute(
                        testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                        ":value2", createStringAttribute("someValue" + TABLE3 + org + "2")))).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE,
                Optional.of(RANGE_KEY_VALUE + "2"),
                SOME_FIELD_VALUE + TABLE3 + org + "2",
                Optional.of(INDEX_FIELD_VALUE))));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryGsi(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3).withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", INDEX_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createStringAttribute(INDEX_FIELD_VALUE)))
                    .withIndexName("testgsi")).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE,
                Optional.of(RANGE_KEY_VALUE + "2"),
                SOME_FIELD_VALUE + TABLE3 + org + "2",
                Optional.of(INDEX_FIELD_VALUE))));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void queryLsi(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression("#name = :value and #name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", INDEX_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createHkAttribute(
                    testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                    ":value2", createStringAttribute(INDEX_FIELD_VALUE)))
                .withIndexName("testlsi");
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertThat(items.get(0), is(buildItemWithValues(
                testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE,
                Optional.of(RANGE_KEY_VALUE + "2"),
                SOME_FIELD_VALUE + TABLE3 + org + "2",
                Optional.of(INDEX_FIELD_VALUE))));
        });
    }

}