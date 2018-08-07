package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildItemWithValues;
import static com.salesforce.dynamodbv2.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author msgroi
 */
class PutTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;
    private static final String HASH_KEY_VALUE_NEW = "2";
    private static final String RANGE_KEY_VALUE_NEW = RANGE_KEY_VALUE + "New";
    private static final String SOME_FIELD_VALUE_NEW = SOME_FIELD_VALUE + "New";
    private static final String SOME_FIELD_VALUE_OVERWRITTEN = SOME_FIELD_VALUE + "Overwritten";

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void put(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            Map<String, AttributeValue> item = buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE_NEW,
                Optional.empty(),
                SOME_FIELD_VALUE_NEW);
            assertNull(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1, HASH_KEY_VALUE_NEW)); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(item);
            testArgument.getAmazonDynamoDB().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1, HASH_KEY_VALUE_NEW), is(item));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void putOverwrite(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1),
                       is(new HashMap<>(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE1 + org)))); // assert before state
            Map<String, AttributeValue> itemToOverwrite = buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE_OVERWRITTEN);
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE1).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDB().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE1, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1), is(itemToOverwrite));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void putHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            Map<String, AttributeValue> item = buildItemWithValues(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW),
                SOME_FIELD_VALUE_NEW);
            assertNull(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3,
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW))); // assert before state
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(item);
            testArgument.getAmazonDynamoDB().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(item))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getItem(testArgument.getHashKeyAttrType(),
                testArgument.getAmazonDynamoDB(),
                TABLE3,
                HASH_KEY_VALUE_NEW,
                Optional.of(RANGE_KEY_VALUE_NEW)),
                       is(item));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void putOverwriteHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3),
                       is(new HashMap<>(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org)))); // assert before state
            Map<String, AttributeValue> itemToOverwrite = buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE_OVERWRITTEN);
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(TABLE3).withItem(itemToOverwrite);
            testArgument.getAmazonDynamoDB().putItem(putItemRequest);
            assertThat(putItemRequest.getItem(), is(new HashMap<>(itemToOverwrite))); // assert no side effects
            assertEquals(TABLE3, putItemRequest.getTableName()); // assert no side effects
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3), is(itemToOverwrite));
        });
    }

}