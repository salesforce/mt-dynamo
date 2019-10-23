package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests putItem().
 *
 * @author msgroi
 */
class PutTest {

    private static final String HASH_KEY_VALUE_NEW = "3";
    private static final String RANGE_KEY_VALUE_NEW = RANGE_KEY_S_VALUE + "New";
    private static final String SOME_FIELD_VALUE_NEW = SOME_FIELD_VALUE + "New";
    private static final String SOME_FIELD_VALUE_OVERWRITTEN = SOME_FIELD_VALUE + "Overwritten";
    private static final String ATTRIBUTE_NOT_EXISTS = "attribute_not_exists";

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void put(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> item = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                        HASH_KEY_VALUE_NEW)
                    .someField(S, SOME_FIELD_VALUE_NEW)
                    .build();
            assertNull(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty())); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(item);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertEquals(new HashMap<>(item), putItemRequest.getItem()); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertEquals(item, getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void putOverwrite(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty())); // assert before state
            Map<String, AttributeValue> itemToOverwrite =
                    ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .someField(S, SOME_FIELD_VALUE_OVERWRITTEN)
                            .build();
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertEquals(new HashMap<>(itemToOverwrite), putItemRequest.getItem()); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertEquals(itemToOverwrite,
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE3})
    void putHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> item = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                        HASH_KEY_VALUE_NEW)
                    .someField(S, SOME_FIELD_VALUE_NEW)
                    .rangeKey(S, RANGE_KEY_VALUE_NEW)
                    .build();
            assertNull(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_VALUE_NEW))); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(item);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertEquals(new HashMap<>(item), putItemRequest.getItem()); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertEquals(item, getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_VALUE_NEW)));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE3})
    void putOverwriteHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            // assert before state
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
            Map<String, AttributeValue> itemToOverwrite =
                    ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .someField(S, SOME_FIELD_VALUE_OVERWRITTEN)
                            .rangeKey(S, RANGE_KEY_S_VALUE)
                            .build();
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertEquals(new HashMap<>(itemToOverwrite), putItemRequest.getItem()); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertEquals(itemToOverwrite,
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void putAttributeNotExists(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE1)
                .withItem(ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                    HASH_KEY_VALUE_NEW)
                    .someField(S, SOME_FIELD_VALUE_NEW)
                    .build())
                .withConditionExpression(ATTRIBUTE_NOT_EXISTS + "(#hk)")
                .withExpressionAttributeNames(ImmutableMap.of("#hk", HASH_KEY_FIELD));
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            try {
                testArgument.getAmazonDynamoDb().putItem(putItemRequest);
                fail("expected exception not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertEquals(e.getMessage(), "The conditional request failed (Service: null; Status Code: 400; "
                    + "Error Code: ConditionalCheckFailedException; Request ID: null)");
            }
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void putMissingHk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE1)
                .withItem(ImmutableMap.of(SOME_FIELD, createStringAttribute(SOME_FIELD_VALUE_NEW)));
            try {
                testArgument.getAmazonDynamoDb().putItem(putItemRequest);
                fail("expected exception not encountered");
            } catch (AmazonServiceException e) {
                assertEquals(e.getMessage(), "One of the required keys was not given a value"
                    + " (Service: null; Status Code: 400; Error Code: ValidationException; Request ID: null)");
            }
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE3})
    void putMissingRk(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE3)
                .withItem(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE_NEW)
                    .someField(S, SOME_FIELD_VALUE_NEW)
                    .build());
            try {
                testArgument.getAmazonDynamoDb().putItem(putItemRequest);
                fail("expected exception not encountered");
            } catch (AmazonServiceException e) {
                assertEquals(e.getMessage(), "One of the required keys was not given a value"
                    + " (Service: null; Status Code: 400; Error Code: ValidationException; Request ID: null)");
            }
        });
    }
}
