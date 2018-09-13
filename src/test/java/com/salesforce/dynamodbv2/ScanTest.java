package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.ORGS_PER_TEST;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.attributeValueToString;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultTestSetup;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests scan().
 *
 * @author msgroi
 */
class ScanTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = ArgumentBuilder.MT_CONTEXT;
    private static final ScanTestSetup scanTestSetup = new ScanTestSetup();

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void scanWithHk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String filterExpression = "#name = :value";
            Map<String, String> expressionAttrNames = ImmutableMap.of("#name", HASH_KEY_FIELD);
            Map<String, AttributeValue> expressionAttrValues = ImmutableMap
                .of(":value", createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            ScanRequest scanRequest = new ScanRequest().withTableName(TABLE1).withFilterExpression(filterExpression)
                .withExpressionAttributeNames(expressionAttrNames)
                .withExpressionAttributeValues(expressionAttrValues);
            assertThat(testArgument.getAmazonDynamoDb().scan(scanRequest).getItems().get(0),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .build()));
            assertEquals(TABLE1, scanRequest.getTableName());
            assertThat(scanRequest.getFilterExpression(), is(filterExpression));
            assertThat(scanRequest.getExpressionAttributeNames(), is(expressionAttrNames));
            assertThat(scanRequest.getExpressionAttributeValues(), is(expressionAttrValues));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void scanWithScanFilter(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> assertThat(testArgument.getAmazonDynamoDb().scan(new ScanRequest()
                        .withTableName(TABLE1)
                        .withScanFilter(ImmutableMap.of(
                                HASH_KEY_FIELD,
                                new Condition().withComparisonOperator(EQ)
                                        .withAttributeValueList(createAttributeValue(testArgument.getHashKeyAttrType(),
                                                HASH_KEY_VALUE))))).getItems().get(0),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .build())));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void scanByNonPk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String filterExpression = "#name = :value";
            Map<String, String> expressionAttrNames = ImmutableMap.of("#name", SOME_FIELD);
            Map<String, AttributeValue> expressionAttrValues = ImmutableMap
                .of(":value", createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org));
            ScanRequest scanRequest = new ScanRequest().withTableName(TABLE1).withFilterExpression(filterExpression)
                .withExpressionAttributeNames(expressionAttrNames)
                .withExpressionAttributeValues(expressionAttrValues);
            assertThat(testArgument.getAmazonDynamoDb().scan(scanRequest).getItems().get(0),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .build()));
            assertEquals(TABLE1, scanRequest.getTableName()); // assert no side effects
            assertThat(scanRequest.getFilterExpression(), is(filterExpression)); // assert no side effects
            assertThat(scanRequest.getExpressionAttributeNames(), is(expressionAttrNames)); // assert no side effects
            assertThat(scanRequest.getExpressionAttributeValues(), is(expressionAttrValues)); // assert no side effects
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void scanAll(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            final Set<Map<String, AttributeValue>> items = new HashSet<>(testArgument.getAmazonDynamoDb()
                .scan(new ScanRequest().withTableName(TABLE1)).getItems());
            assertEquals(2, items.size());
            final Map<String, AttributeValue> someValue = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                        HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build();
            final Map<String, AttributeValue> someOtherValue = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                        HASH_KEY_OTHER_VALUE)
                    .someField(S, SOME_OTHER_OTHER_FIELD_VALUE + TABLE1 + org)
                    .build();
            final ImmutableSet<Map<String, AttributeValue>> expectedSet = ImmutableSet.of(someValue, someOtherValue);
            assertEquals(expectedSet, items);
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(ScanTestArgumentProvider.class)
    void scanWithPaging(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> scanAndAssertItemKeys(scanTestSetup.orgItemKeys.get(org),
            testArgument.getAmazonDynamoDb(),
            testArgument.getHashKeyAttrType()));
    }

    private void scanAndAssertItemKeys(Set<Integer> expectedItems,
        AmazonDynamoDB amazonDynamoDb,
        ScalarAttributeType hashKeyAttrType) {
        Map<String, AttributeValue> exclusiveStartKey = null;
        do {
            ScanResult scanResult = amazonDynamoDb
                .scan(new ScanRequest(TABLE1).withLimit(10).withExclusiveStartKey(exclusiveStartKey));
            exclusiveStartKey = scanResult.getLastEvaluatedKey();
            List<Map<String, AttributeValue>> items = scanResult.getItems();

            if (items.isEmpty()) {
                assertTrue(expectedItems.isEmpty());
                assertNull(exclusiveStartKey);
            } else {
                assertTrue(items.stream()
                    .map(i -> i.get(HASH_KEY_FIELD))
                    .map(i -> attributeValueToString(hashKeyAttrType, i))
                    .map(Integer::parseInt)
                    .allMatch(expectedItems::remove));
            }
        } while (exclusiveStartKey != null);
        assertTrue(expectedItems.isEmpty());
    }

    private static class ScanTestSetup extends DefaultTestSetup {
        List<Integer> orgPutCounts = ImmutableList.of(100, 10, 0);
        Map<String,Set<Integer>> orgItemKeys = new HashMap<>();

        @Override
        public void setupTableData(AmazonDynamoDB amazonDynamoDb, ScalarAttributeType hashKeyAttrType, String org,
            CreateTableRequest createTableRequest) {
            int ordinal = (Integer.parseInt(org.substring(org.indexOf("-") + 1)) - 1) % ORGS_PER_TEST;
            int putCount = ordinal < orgPutCounts.size() ? orgPutCounts.get(ordinal) : new Random().nextInt(10);
            MT_CONTEXT.setContext(org);
            Set<Integer> itemKeys = new HashSet<>();
            orgItemKeys.put(org, itemKeys);
            // insert some data for another tenant as noise
            for (int i = 0; i < putCount; i++) {
                amazonDynamoDb.putItem(
                    new PutItemRequest(TABLE1, ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(
                        hashKeyAttrType, String.valueOf(i)))));
                itemKeys.add(i);
            }
        }
    }

    /*
     * Replaces the default data setup with one that is specific to the ScanTest's paging test.
     */
    static class ScanTestArgumentProvider extends DefaultArgumentProvider {

        public ScanTestArgumentProvider() {
            super(scanTestSetup);
        }

    }

}
