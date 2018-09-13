package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
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
    private static final String RANGE_KEY_VALUE_NEW = RANGE_KEY_VALUE + "New";
    private static final String SOME_FIELD_VALUE_NEW = SOME_FIELD_VALUE + "New";
    private static final String SOME_FIELD_VALUE_OVERWRITTEN = SOME_FIELD_VALUE + "Overwritten";

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
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
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                    is(item));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void putOverwrite(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .build())); // assert before state
            Map<String, AttributeValue> itemToOverwrite =
                    ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .someField(S, SOME_FIELD_VALUE_OVERWRITTEN)
                            .build();
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                    is(itemToOverwrite));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
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
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE_NEW,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_VALUE_NEW)),
                is(item));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void putOverwriteHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            // assert before state
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_VALUE)),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                        .rangeKey(S, RANGE_KEY_VALUE)
                        .build()));
            Map<String, AttributeValue> itemToOverwrite =
                    ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .someField(S, SOME_FIELD_VALUE_OVERWRITTEN)
                            .rangeKey(S, RANGE_KEY_VALUE)
                            .build();
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDb().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_VALUE)),
                is(itemToOverwrite));
        });
    }

}
