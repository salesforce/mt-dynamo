package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_STRING_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItemDefaultHkRk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
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
    private static final String RANGE_KEY_VALUE_NEW = RANGE_KEY_STRING_VALUE + "New";
    private static final String SOME_FIELD_VALUE_NEW = SOME_FIELD_VALUE + "New";
    private static final String SOME_FIELD_VALUE_OVERWRITTEN = SOME_FIELD_VALUE + "Overwritten";

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void put(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> item = buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE_NEW,
                Optional.empty(),
                SOME_FIELD_VALUE_NEW);
            assertNull(getItem(testArgument.getHashKeyAttrType(),
                testArgument.getAmazonDynamoDb(), TABLE1, HASH_KEY_VALUE_NEW)); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(item);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(),
                testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE_NEW), is(item));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void putOverwrite(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                is(new HashMap<>(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                    SOME_FIELD_VALUE + TABLE1 + org)))); // assert before state
            Map<String, AttributeValue> itemToOverwrite =
                buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE_OVERWRITTEN);
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                is(itemToOverwrite));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void putHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> item = buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW),
                SOME_FIELD_VALUE_NEW);
            assertNull(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3,
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW))); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(item);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(),
                testArgument.getAmazonDynamoDb(),
                TABLE3,
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW)),
                is(item));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void putOverwriteHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            // assert before state
            assertThat(getItemDefaultHkRk(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                is(new HashMap<>(buildHkRkItemWithSomeFieldValue(
                    testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org))));
            Map<String, AttributeValue> itemToOverwrite =
                buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE_OVERWRITTEN);
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItemDefaultHkRk(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                is(itemToOverwrite));
        });
    }

}