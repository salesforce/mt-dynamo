package com.salesforce.dynamodbv2.parameterizedtests;

import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ParameterizedTestArgumentProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests putItem().
 *
 * @author msgroi
 */
@Tag("parameterized-tests")
class ParameterizedPutTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;
    private static final String HASH_KEY_VALUE_NEW = "2";
    private static final String RANGE_KEY_VALUE_NEW = RANGE_KEY_VALUE + "New";
    private static final String SOME_FIELD_VALUE_NEW = SOME_FIELD_VALUE + "New";
    private static final String SOME_FIELD_VALUE_OVERWRITTEN = SOME_FIELD_VALUE + "Overwritten";

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void put(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
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

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void putOverwrite(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
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

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void putHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
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

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
    void putOverwriteHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            // assert before state
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(new HashMap<>(buildHkRkItemWithSomeFieldValue(
                           testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org))));
            Map<String, AttributeValue> itemToOverwrite =
                buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE_OVERWRITTEN);
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(itemToOverwrite));
        });
    }

}