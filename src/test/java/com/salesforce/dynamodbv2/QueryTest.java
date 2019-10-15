package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
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
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests query().
 *
 * <p>Some tests use Dynalite, because DynamoDB Local incorrectly enforces that the aggregated size of all range key
 * values in a key condition expression cannot exceed 1024 bytes, when the limit should be per value only, causing
 * hash partitioning tests to fail. Tests that involve paging also trigger a second bug, where binary values in query
 * key conditions are being compared as signed bytes rather than unsigned bytes.
 *
 * @author msgroi
 */
class QueryTest {

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
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
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void queryWithKeyCondition(TestArgument testArgument) {
        runHkTableKeyConditionTest(testArgument, false);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void queryWithKeyCondition_legacy(TestArgument testArgument) {
        runHkTableKeyConditionTest(testArgument, true);
    }

    private void runHkTableKeyConditionTest(TestArgument testArgument, boolean useLegacy) {
        testArgument.forEachOrgContext(org -> {
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE1);
            setHashKeyCondition(queryRequest, testArgument.getHashKeyAttrType(), useLegacy);
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            final Set<Map<String, AttributeValue>> expectedItemsSet = ImmutableSet.of(ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                .build());
            assertEquals(expectedItemsSet, new HashSet<>(items));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE4}, useDynalite = true)
    void queryHkRkWithKeyConditions(TestArgument testArgument) {
        runHkRkTableKeyConditionTest(testArgument, false);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE4}, useDynalite = true)
    void queryHkRkWithKeyConditions_legacy(TestArgument testArgument) {
        runHkRkTableKeyConditionTest(testArgument, true);
    }

    private void runHkRkTableKeyConditionTest(TestArgument testArgument, boolean useLegacy) {
        // Table has (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX)

        // RK condition: none, expected: (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX)
        runHkRkTableKeyConditionTest(testArgument,
            queryRequest -> setHashKeyCondition(queryRequest, testArgument.getHashKeyAttrType(), useLegacy),
            IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MAX),
            "RK condition: none, expected: (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX)");

        // RK condition: rk > RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, GT, ImmutableList.of(RANGE_KEY_N_MIN),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX),
            "RK condition: rk > RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)");

        // RK condition: rk > RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, GT, ImmutableList.of(RANGE_KEY_N_MIN + 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 2, RANGE_KEY_N_MAX),
            "RK condition: rk > RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)");

        // RK condition: rk >= RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, GE, ImmutableList.of(RANGE_KEY_N_MIN),
            IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MAX),
            "RK condition: rk >= RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN), ..., (hk, RANGE_KEY_N_MAX)");

        // RK condition: rk >= RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, GE, ImmutableList.of(RANGE_KEY_N_MIN + 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX),
            "RK condition: rk >= RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX)");

        // RK condition: rk < RANGE_KEY_N_MIN, expected: NONE
        runHkRkTableKeyConditionTest(testArgument, useLegacy, LT, ImmutableList.of(RANGE_KEY_N_MIN),
            Collections.emptySet(), "RK condition: rk < RANGE_KEY_N_MIN, expected: NONE");

        // RK condition: rk < RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, LT, ImmutableList.of(RANGE_KEY_N_MIN + 1),
            ImmutableSet.of(RANGE_KEY_N_MIN),
            "RK condition: rk < RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN)");

        // RK condition: rk <= RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, LE, ImmutableList.of(RANGE_KEY_N_MIN),
            IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MIN),
            "RK condition: rk <= RANGE_KEY_N_MIN, expected: (hk, RANGE_KEY_N_MIN)");

        // RK condition: rk <= RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN) and (hk, RANGE_KEY_N_MIN + 1)
        runHkRkTableKeyConditionTest(testArgument, useLegacy, LE, ImmutableList.of(RANGE_KEY_N_MIN + 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MIN + 1),
            "RK condition: rk <= RANGE_KEY_N_MIN + 1, expected: (hk, RANGE_KEY_N_MIN) and (hk, RANGE_KEY_N_MIN + 1)");

        // RK condition: rk BETWEEN RANGE_KEY_N_MIN + 1 AND RANGE_KEY_N_MAX - 1
        // expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX - 1)
        // TODO: random partitioning doesn't support BETWEEN yet
        /*runHkRkTableKeyConditionTest(testArgument, useLegacy, BETWEEN,
            ImmutableList.of(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX - 1),
            IntStream.rangeClosed(RANGE_KEY_N_MIN + 1, RANGE_KEY_N_MAX - 1),
            "RK condition: rk BETWEEN RANGE_KEY_N_MIN + 1 AND RANGE_KEY_N_MAX - 1, "
                + "expected: (hk, RANGE_KEY_N_MIN + 1), ..., (hk, RANGE_KEY_N_MAX - 1)");*/
    }

    private void runHkRkTableKeyConditionTest(TestArgument testArgument,
                                              boolean useLegacyKeyConditions,
                                              ComparisonOperator op,
                                              List<Integer> comparisonValues,
                                              IntStream expectedValues,
                                              String errorMessage) {
        runHkRkTableKeyConditionTest(testArgument, useLegacyKeyConditions, op, comparisonValues,
            expectedValues.mapToObj(Integer::valueOf).collect(Collectors.toSet()), errorMessage);
    }

    private void runHkRkTableKeyConditionTest(TestArgument testArgument,
                                              Consumer<QueryRequest> queryRequestModifier,
                                              IntStream expectedValues,
                                              String errorMessage) {
        runHkRkTableKeyConditionTest(testArgument, queryRequestModifier,
            expectedValues.mapToObj(Integer::valueOf).collect(Collectors.toSet()), errorMessage);
    }

    private void runHkRkTableKeyConditionTest(TestArgument testArgument,
                                              boolean useLegacyKeyConditions,
                                              ComparisonOperator op,
                                              List<Integer> comparisonValues,
                                              Set<Integer> expectedValues,
                                              String errorMessage) {
        runHkRkTableKeyConditionTest(testArgument,
            queryRequest -> setHashAndRangeKeyConditions(queryRequest, testArgument.getHashKeyAttrType(), op,
                comparisonValues, useLegacyKeyConditions),
            expectedValues,
            errorMessage);
    }

    private void runHkRkTableKeyConditionTest(TestArgument testArgument,
                                              Consumer<QueryRequest> queryRequestModifier,
                                              Set<Integer> expectedValues,
                                              String errorMessage) {
        testArgument.forEachOrgContext(org -> {
            final QueryRequest queryRequest = new QueryRequest()
                .withTableName(TABLE4);
            queryRequestModifier.accept(queryRequest);
            final List<Map<String, AttributeValue>> items = testArgument
                .getAmazonDynamoDb()
                .query(queryRequest)
                .getItems();
            final Set<Map<String, AttributeValue>> expectedItemSet = expectedValues.stream()
                .map(rangeKey -> ItemBuilder
                    .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(N, String.valueOf(rangeKey))
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE4 + org)
                    .build())
                .collect(Collectors.toSet());
            assertEquals(expectedItemSet, new HashSet<>(items), errorMessage);
        });
    }

    private void setHashKeyCondition(QueryRequest queryRequest, ScalarAttributeType hashKeyType,
                                     boolean useLegacyKeyConditions) {
        AttributeValue hkAttributeValue = createAttributeValue(hashKeyType, HASH_KEY_VALUE);
        if (useLegacyKeyConditions) {
            queryRequest.setKeyConditions(ImmutableMap.of(
                HASH_KEY_FIELD, new Condition().withComparisonOperator(EQ).withAttributeValueList(hkAttributeValue)));
        } else {
            String keyConditionExpression = HASH_KEY_FIELD + " = :hk";
            queryRequest.setKeyConditionExpression(keyConditionExpression);
            queryRequest.addExpressionAttributeValuesEntry(":hk", hkAttributeValue);
        }
    }

    private void setHashAndRangeKeyConditions(QueryRequest queryRequest, ScalarAttributeType hashKeyType,
                                              ComparisonOperator rangeKeyOp, List<Integer> rangeKeyValues,
                                              boolean useLegacyKeyConditions) {
        AttributeValue hkAttributeValue = createAttributeValue(hashKeyType, HASH_KEY_VALUE);
        AttributeValue[] rkAttributeValues = rangeKeyValues.stream()
            .map(v -> createAttributeValue(N, String.valueOf(v)))
            .toArray(AttributeValue[]::new);
        if (useLegacyKeyConditions) {
            queryRequest.setKeyConditions(getKeyConditions(hkAttributeValue, rangeKeyOp, rkAttributeValues));
        } else {
            String keyConditionExpression = HASH_KEY_FIELD + " = :hk AND " + getRangeKeyConditionExpression(rangeKeyOp);
            queryRequest.setKeyConditionExpression(keyConditionExpression);
            queryRequest.addExpressionAttributeValuesEntry(":hk", hkAttributeValue);
            for (int i = 0; i < rkAttributeValues.length; i++) {
                queryRequest.addExpressionAttributeValuesEntry(":rk" + i, rkAttributeValues[i]);
            }
        }
    }

    private String getRangeKeyConditionExpression(ComparisonOperator op) {
        switch (op) {
            case EQ:
                return RANGE_KEY_FIELD + " = :rk0";
            case LT:
                return RANGE_KEY_FIELD + " < :rk0";
            case LE:
                return RANGE_KEY_FIELD + " <= :rk0";
            case GT:
                return RANGE_KEY_FIELD + " > :rk0";
            case GE:
                return RANGE_KEY_FIELD + " >= :rk0";
            case BETWEEN:
                return RANGE_KEY_FIELD + " BETWEEN :rk0 AND :rk1";
            default:
                throw new IllegalArgumentException("Comparison operator is not supported: " + op);
        }
    }

    private Map<String, Condition> getKeyConditions(ScalarAttributeType hashKeyType, ComparisonOperator rangeKeyOp,
                                                    AttributeValue... rkValues) {
        AttributeValue hkValue = createAttributeValue(hashKeyType, HASH_KEY_VALUE);
        return ImmutableMap.of(
            HASH_KEY_FIELD, new Condition().withComparisonOperator(EQ).withAttributeValueList(hkValue),
            RANGE_KEY_FIELD, new Condition().withComparisonOperator(rangeKeyOp).withAttributeValueList(rkValues)
        );
    }

    private Map<String, Condition> getKeyConditions(AttributeValue hkValue, ComparisonOperator rangeKeyOp,
                                                    AttributeValue... rkValues) {
        return ImmutableMap.of(
            HASH_KEY_FIELD, new Condition().withComparisonOperator(EQ).withAttributeValueList(hkValue),
            RANGE_KEY_FIELD, new Condition().withComparisonOperator(rangeKeyOp).withAttributeValueList(rkValues)
        );
    }

    // TODO: non-legacy query test(s); legacy & non-legacy scan tests
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
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
    @DefaultArgumentProviderConfig(tables = {TABLE1})
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
    @DefaultArgumentProviderConfig(tables = {TABLE3}, useDynalite = true)
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
    @DefaultArgumentProviderConfig(tables = {TABLE3}, useDynalite = true)
    void queryHkRkWithFilterExpression_lessThanOrEqual(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3).withKeyConditionExpression("#name = :value")
                    .withFilterExpression("#name2 <= :value2")
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
    @DefaultArgumentProviderConfig(tables = {TABLE3})
    void queryGsi(TestArgument testArgument) {
        runQueryGsiTest(testArgument, false);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE3}, useDynalite = true)
    void queryGsiWithRk(TestArgument testArgument) {
        runQueryGsiTest(testArgument, true);
    }

    private void runQueryGsiTest(TestArgument testArgument, boolean testHkRkGsi) {
        String indexName = testHkRkGsi ? "testGsi2" : "testGsi";
        String hkField = testHkRkGsi ? GSI2_HK_FIELD : GSI_HK_FIELD;
        String hkValue = testHkRkGsi ? GSI2_HK_FIELD_VALUE : GSI_HK_FIELD_VALUE;
        testArgument.forEachOrgContext(org -> {
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(TABLE3)
                    .withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", hkField))
                    .withExpressionAttributeValues(ImmutableMap.of(":value", createStringAttribute(hkValue)))
                    .withIndexName(indexName)).getItems();
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
    @DefaultArgumentProviderConfig(tables = {TABLE5})
    void queryGsi_TableWithGsiHkSameAsTableRk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String table = TABLE5;
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(
                new QueryRequest().withTableName(table).withKeyConditionExpression("#name = :value")
                    .withExpressionAttributeNames(ImmutableMap.of("#name", RANGE_KEY_FIELD))
                    .withExpressionAttributeValues(ImmutableMap.of(
                        ":value", createStringAttribute(RANGE_KEY_OTHER_S_VALUE),
                        ":hkValue", createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    ))
                    .withFilterExpression(HASH_KEY_FIELD + " = :hkValue")
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
    @DefaultArgumentProviderConfig(tables = {TABLE3}, useDynalite = true)
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
    @DefaultArgumentProviderConfig(tables = {TABLE3})
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

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE4}, useDynalite = true)
    void queryWithPaging(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
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
                    .withKeyConditions(
                        getKeyConditions(testArgument.getHashKeyAttrType(), GT, createAttributeValue(N, "1")))
                    .withLimit(maxPageSize)
                    .withExclusiveStartKey(exclusiveStartKey);
                final QueryResult queryResult = testArgument.getAmazonDynamoDb().query(queryRequest);
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
        });
    }

    // return the ceiling of the result of dividing {@code numerator} by {@code denominator}.
    private static int intCeilFrac(int numerator, int denominator) {
        return (numerator + denominator - 1) / denominator;
    }

    /**
     * This test highlights the bug in DynamoDB Local where binary values in query key conditions are not being compared
     * as unsigned bytes. (Most other tests here that use Dynalite fail only when using hash partitioning due to wrong
     * enforcement of range key value sizes, though the tests involving paging would still fail due to this issue even
     * if the range key size issue is fixed. This test fails for all shared table strategies when using DynamoDB Local.)
     * Using Dynalite as a workaround for now.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(useDynalite = true)
    void queryBinaryExclusiveStartKey(TestArgument testArgument) {
        String tableName = "MyTable";
        AmazonDynamoDB amazonDynamoDb = testArgument.getAmazonDynamoDb();
        testArgument.forEachOrgContext(org -> {
            CreateTableRequest createTableRequest = CreateTableRequestBuilder.builder()
                .withTableName(tableName)
                .withAttributeDefinitions(
                    new AttributeDefinition(HASH_KEY_FIELD, testArgument.getHashKeyAttrType()),
                    new AttributeDefinition(RANGE_KEY_FIELD, B))
                .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, B)
                .build();
            new AmazonDynamoDbAdminUtils(amazonDynamoDb).createTableIfNotExists(createTableRequest, 0);

            ByteBuffer startKey = ByteBuffer.wrap(new byte[]{0, 1, 2});
            ByteBuffer upperBound = ByteBuffer.wrap(new byte[]{0, 1, -1});
            amazonDynamoDb.query(
                new QueryRequest().withTableName(tableName)
                    .withKeyConditionExpression(HASH_KEY_FIELD + " = :hk AND " + RANGE_KEY_FIELD + " <= :rk")
                    .addExpressionAttributeValuesEntry(":hk",
                        createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                    .addExpressionAttributeValuesEntry(":rk", new AttributeValue().withB(upperBound))
                    .withExclusiveStartKey(ImmutableMap.of(
                        HASH_KEY_FIELD, createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                        RANGE_KEY_FIELD, new AttributeValue().withB(startKey)))
            );
        });
    }

}
