package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE4;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_RK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_N_MAX;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_N_MIN;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_OTHER_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.attributeValueToString;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests query().
 *
 * @author msgroi
 */
class QueryTest {
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void query(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String keyConditionExpression = "#name = :value";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#name", HASH_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap
                .of(":value", createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE1)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build(),
                items.get(0));
            assertEquals(TABLE1, queryRequest.getTableName()); // assert no side effects
            assertEquals(keyConditionExpression, queryRequest.getKeyConditionExpression()); // assert no side effects
            // assert no side effects
            assertEquals(queryExpressionAttrNames, queryRequest.getExpressionAttributeNames());
            // assert no side effects
            assertEquals(queryExpressionAttrValues, queryRequest.getExpressionAttributeValues());
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsEq(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb()
                .query(new QueryRequest().withTableName(TABLE1)
                    .withKeyConditions(ImmutableMap.of(
                        HASH_KEY_FIELD,
                        new Condition().withComparisonOperator(EQ).withAttributeValueList(createAttributeValue(
                            testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))))
                ).getItems();
            final Set<Map<String, AttributeValue>> expectedItemsSet = ImmutableSet.of(ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                .build());
            assertEquals(expectedItemsSet, new HashSet<>(items));
        });
    }


    // test legacy calls (see {@code QueryMapper} for more on "legacy".

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk > RANGE_KEY_N_MIN, so there should be RANGE_KEY_N_MAX - RANGE_KEY_N_MIN results:
     * (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsGtLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(GT,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX)
                .mapToObj(String::valueOf)
                .collect(Collectors.toSet()));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk > RANGE_KEY_N_MIN + 1, so there should be RANGE_KEY_N_MAX - (RANGE_KEY_N_MIN + 1) results:
     * (hk, RANGE_KEY_N_MIN + 2), ..., (hk, RANGE_KEY_N_MAX).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsGtAlmostAsLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(GT,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN + 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 2, RANGE_KEY_N_MAX)
                .mapToObj(String::valueOf)
                .collect(Collectors.toSet()));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk >= RANGE_KEY_N_MIN, so there should be RANGE_KEY_N_MAX - RANGE_KEY_N_MIN + 1 results:
     * (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsGeLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(GE,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN),
            IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MAX)
                .mapToObj(String::valueOf)
                .collect(Collectors.toSet()));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk >= RANGE_KEY_N_MIN + 1, so there should be RANGE_KEY_N_MAX - RANGE_KEY_N_MIN results:
     * (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsGeAlmostAsLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(GE,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN + 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX)
                .mapToObj(String::valueOf)
                .collect(Collectors.toSet()));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk < RANGE_KEY_N_MIN, so there should be 0 results.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsLtLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(LT,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN),
            ImmutableSet.of());
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk < RANGE_KEY_N_MIN + 1, so there should be 1 result: (hk, RANGE_KEY_N_MIN).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsLtAlmostAsLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(LT,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN + 1),
            ImmutableSet.of(String.valueOf(RANGE_KEY_N_MIN)));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk <= RANGE_KEY_N_MIN, so there should be 1 result: (hk, RANGE_KEY_N_MIN).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsLeLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(LE,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN),
            ImmutableSet.of(String.valueOf(RANGE_KEY_N_MIN)));
    }

    /**
     * Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX); we ask for items that match hk and have
     * rk <= RANGE_KEY_N_MIN + 1, so there should be 2 results: (hk, RANGE_KEY_N_MIN) and (hk, RANGE_KEY_N_MIN + 1).
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithKeyConditionsLeAlmostAsLow(TestArgument testArgument) {
        queryWithKeyConditionsComparisonOperatorInner(LE,
            testArgument,
            String.valueOf(RANGE_KEY_N_MIN + 1),
            ImmutableSet.of(String.valueOf(RANGE_KEY_N_MIN), String.valueOf(RANGE_KEY_N_MIN + 1)));
    }

    // see any caller
    private void queryWithKeyConditionsComparisonOperatorInner(ComparisonOperator op,
                                                               TestArgument testArgument,
                                                               String valueForComparisonOperator,
                                                               Set<String> expectedRangeKeyValues) {
        testArgument.forEachOrgContext(org -> {
            final QueryRequest queryRequest = new QueryRequest()
                .withTableName(TABLE4)
                .withKeyConditions(getKeyConditions(op, valueForComparisonOperator, testArgument.getHashKeyAttrType()));
            final List<Map<String, AttributeValue>> items = testArgument
                .getAmazonDynamoDb()
                .query(queryRequest)
                .getItems();
            final Set<Map<String, AttributeValue>> expectedItemsSet = expectedRangeKeyValues.stream()
                .map(rangeKey -> ItemBuilder
                    .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(N, rangeKey)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE4 + org)
                    .build())
                .collect(Collectors.toSet());
            assertEquals(expectedItemsSet, new HashSet<>(items));
        });
    }

    private Map<String, Condition> getKeyConditions(ComparisonOperator op,
                                                    String valueForComparisonOperator,
                                                    ScalarAttributeType hashKeyAttrType) {
        return ImmutableMap.of(
            HASH_KEY_FIELD,
            new Condition().withComparisonOperator(EQ).withAttributeValueList(createAttributeValue(
                hashKeyAttrType, HASH_KEY_VALUE)),
            RANGE_KEY_FIELD,
            new Condition().withComparisonOperator(op).withAttributeValueList(createAttributeValue(
                N, valueForComparisonOperator)));
    }

    // TODO: non-legacy query test(s); legacy & non-legacy scan tests
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryUsingAttributeNamePlaceholders(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE1).withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createAttributeValue(
                        testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)))).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build(),
                items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    // Note: field names with '-' will fail if you use literals instead of expressionAttributeNames()
    void queryUsingAttributeNameLiterals(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items =
                testArgument.getAmazonDynamoDb().query(new QueryRequest().withTableName(TABLE1)
                    .withKeyConditionExpression(HASH_KEY_FIELD + " = :value")
                    .withExpressionAttributeValues(ImmutableMap.of(":value",
                        createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)))).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build(),
                items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String keyConditionExpression = "#hk = :hkv and #rk = :rkv";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of(
                "#hk", HASH_KEY_FIELD,
                "#rk", RANGE_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap.of(
                ":hkv", createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                ":rkv", createStringAttribute(RANGE_KEY_S_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                items.get(0));
            assertEquals(TABLE3, queryRequest.getTableName()); // assert no side effects
            assertEquals(keyConditionExpression, queryRequest.getKeyConditionExpression()); // assert no side effects
            // assert no side effects
            assertEquals(queryExpressionAttrNames, queryRequest.getExpressionAttributeNames());
            // assert no side effects
            assertEquals(queryExpressionAttrValues, queryRequest.getExpressionAttributeValues());
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryHkRkTableNoRkSpecified(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String keyConditionExpression = "#hk = :hkv";
            Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#hk", HASH_KEY_FIELD);
            Map<String, AttributeValue> queryExpressionAttrValues =
                ImmutableMap.of(":hkv", createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(2, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                items.get(0));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .withDefaults()
                    .build(),
                items.get(1));
            assertEquals(TABLE3, queryRequest.getTableName()); // assert no side effects
            assertEquals(keyConditionExpression, queryRequest.getKeyConditionExpression()); // assert no side effects
            // assert no side effects
            assertEquals(queryExpressionAttrNames, queryRequest.getExpressionAttributeNames());
            // assert no side effects
            assertEquals(queryExpressionAttrValues, queryRequest.getExpressionAttributeValues());
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryHkRkWithFilterExpression(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3).withKeyConditionExpression("#name = :value")
                    .withFilterExpression("#name2 = :value2")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", SOME_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createAttributeValue(
                        testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                        ":value2", createStringAttribute(SOME_OTHER_FIELD_VALUE + TABLE3 + org)))).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .withDefaults()
                    .build(),
                items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryGsi(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3).withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", GSI_HK_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createStringAttribute(GSI_HK_FIELD_VALUE)))
                    .withIndexName("testGsi")).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .withDefaults()
                    .build(),
                items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryGsi_TableWithGsiHkSameAsTableRk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String table = TABLE5;
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                    new QueryRequest().withTableName(table).withKeyConditionExpression("#name = :value")
                            .withExpressionAttributeNames(ImmutableMap.of("#name", RANGE_KEY_FIELD))
                            .withExpressionAttributeValues(ImmutableMap.of(":value",
                                    createStringAttribute(RANGE_KEY_OTHER_S_VALUE)))
                            .withIndexName("testGsi_table_rk_as_index_hk")).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
                .withDefaults()
                .build(), items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryGsiWithPaging(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            // add another gsi2 record so there are multiple and we'd need paging
            Map<String, AttributeValue> secondRecord = ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .withDefaults()
                .rangeKey(S, RANGE_KEY_OTHER_S_VALUE + "1")
                .gsi2HkField(S, GSI2_HK_FIELD_VALUE)
                .gsi2RkField(N, GSI2_RK_FIELD_VALUE + "1")
                .build();
            testArgument.getAmazonDynamoDb().putItem(new PutItemRequest().withTableName(TABLE3).withItem(secondRecord));

            Map<String, AttributeValue> firstRecord = ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .withDefaults()
                .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                .build();
            List<Map<String, AttributeValue>> expectedRecords = ImmutableList.of(firstRecord, secondRecord);

            assertEquals(expectedRecords, executeQueryWithPaging(
                exclusiveStartKey -> testArgument.getAmazonDynamoDb().query(
                    new QueryRequest(TABLE3)
                        .withIndexName("testGsi2")
                        .withKeyConditionExpression("#name = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", GSI2_HK_FIELD))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                            createStringAttribute(GSI2_HK_FIELD_VALUE)))
                        .withLimit(1)
                        .withExclusiveStartKey(exclusiveStartKey))));
        });
    }

    /**
     * Executes a ScanRequest and iterates through pages until a scan result that has an empty lastEvaluatedKey
     * is found.
     */
    private List<Map<String, AttributeValue>> executeQueryWithPaging(
        Function<Map<String, AttributeValue>, QueryResult> queryExecutor) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> exclusiveStartKey = null;
        do {
            QueryResult scanResult = queryExecutor.apply(exclusiveStartKey);
            exclusiveStartKey = scanResult.getLastEvaluatedKey();
            items.addAll(scanResult.getItems());
        } while (exclusiveStartKey != null);
        return items;
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryLsi(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression("#name = :value and #name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", INDEX_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createAttributeValue(
                    testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                    ":value2", createStringAttribute(INDEX_FIELD_VALUE)))
                .withIndexName("testLsi");
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(1, items.size());
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                .withDefaults()
                .build(), items.get(0));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryWithPaging(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> queryAndAssertItemKeys(testArgument.getAmazonDynamoDb(),
            testArgument.getHashKeyAttrType()));
    }

    /**
     * This test specifically addresses a bug whereby two different tables for the same tenant that
     * happen to have the same GSI name would cause unexpected results when the GSI was queried.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void queryGsiTwoTablesSameIndexName(TestArgument testArgument) {
        String gsiHkField = "gsiHk";
        String gsiHkFieldValue = "gsiHkValue";
        String tablePrefix = "queryGsiTwoTablesSameIndexNameTable";
        String gsiName = "testGsi";
        int tableCount = 2;

        AmazonDynamoDB amazonDynamoDb = testArgument.getAmazonDynamoDb();
        testArgument.forEachOrgContext(org -> {
            CreateTableRequestBuilder requestBuilder = CreateTableRequestBuilder.builder()
                .withAttributeDefinitions(
                    new AttributeDefinition(HASH_KEY_FIELD, testArgument.getHashKeyAttrType()),
                    new AttributeDefinition(gsiHkField, S))
                .withTableKeySchema(HASH_KEY_FIELD, S)
                .addSi(gsiName, GSI, new PrimaryKey(gsiHkField, S), 1L);

            for (int i = 0; i < tableCount; i++) {
                // create two tables with the same index name
                String tableName = tablePrefix + i;
                testArgument.getAmazonDynamoDb().createTable(
                    requestBuilder.withTableName(tableName).build());
                // insert an item into two different tables with the same value in the gsiHkField field
                amazonDynamoDb.putItem(
                    new PutItemRequest().withTableName(tableName).withItem(
                        ImmutableMap.of(
                            HASH_KEY_FIELD, createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE + i),
                            gsiHkField, createStringAttribute(gsiHkFieldValue))));
            }

            // perform a query using the GSI
            for (int i = 0; i < tableCount; i++) {
                String tableName = tablePrefix + i;
                List<Map<String, AttributeValue>> items = amazonDynamoDb.query(
                    new QueryRequest().withTableName(tableName).withKeyConditionExpression("#name = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", gsiHkField))
                        .withExpressionAttributeValues(ImmutableMap.of(":value",
                            createStringAttribute(gsiHkFieldValue)))
                        .withIndexName(gsiName)).getItems();
                assertEquals(1, items.size());
                assertEquals(createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE + i),
                    items.get(0).get(HASH_KEY_FIELD));
            }
        });
    }

    private void queryAndAssertItemKeys(AmazonDynamoDB amazonDynamoDb, ScalarAttributeType hashKeyAttrType) {
        int maxPageSize = 4;
        final Set<Integer> expectedItems = IntStream.rangeClosed(2, RANGE_KEY_N_MAX)
            .boxed()
            .collect(Collectors.toSet());
        final int expectedReadCount = expectedItems.size();

        Map<String, AttributeValue> exclusiveStartKey = null;
        int pageCount = 0;
        do {
            ++pageCount;
            final QueryRequest queryRequest = new QueryRequest()
                .withTableName(TABLE4)
                .withKeyConditions(getKeyConditions(GT, "1", hashKeyAttrType))
                .withLimit(maxPageSize)
                .withExclusiveStartKey(exclusiveStartKey);
            final QueryResult queryResult = amazonDynamoDb.query(queryRequest);
            exclusiveStartKey = queryResult.getLastEvaluatedKey();
            final List<Map<String, AttributeValue>> items = queryResult.getItems();

            if (items.isEmpty()) {
                assertTrue(expectedItems.isEmpty(), "Some expected items were not returned: " + expectedItems);
                assertNull(exclusiveStartKey);
            } else {
                assertTrue(items.stream()
                    .map(i -> i.get(RANGE_KEY_FIELD))
                    .map(i -> attributeValueToString(N, i)) // TABLE4's rk is of type N
                    .map(Integer::parseInt)
                    .allMatch(expectedItems::remove));
            }
        } while (exclusiveStartKey != null);

        assertEquals(intCeilFrac(expectedReadCount, maxPageSize), pageCount);
        assertTrue(expectedItems.isEmpty(), "Some expected items were not returned: " + expectedItems);
    }

    // return the ceiling of the result of dividing {@code numerator} by {@code denominator}.
    private static int intCeilFrac(int numerator, int denominator) {
        return (numerator + denominator - 1) / denominator;
    }
}
