package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkKey;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildKey;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests updateItem().
 *
 * @author msgroi
 */
class UpdateTest {

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void update(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = buildKey(testArgument.getHashKeyAttrType());
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate()
                        .withValue(createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(updateItemRequest.getKey(), is(new HashMap<>(updateItemKey))); // assert no side effects
            assertEquals(TABLE1, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(buildKey(testArgument.getHashKeyAttrType()))
                .withUpdateExpression("set #name = :newValue")
                .withConditionExpression("#name = :currentValue")
                .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                    SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalFail(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE1)
                    .withKey(buildKey(testArgument.getHashKeyAttrType()))
                    .withUpdateExpression("set #name = :newValue")
                    .withConditionExpression("#name = :currentValue")
                    .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                    .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
                    .addExpressionAttributeValuesEntry(":newValue",
                        createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
                throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertTrue(e.getMessage().contains("ConditionalCheckFailedException"));
            }
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = buildHkRkKey(testArgument.getHashKeyAttrType());
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate().withValue(
                        createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(updateItemRequest.getKey(), is(new HashMap<>(updateItemKey))); // assert no side effects
            assertEquals(TABLE3, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalSuccessHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(buildHkRkKey(testArgument.getHashKeyAttrType()))
                .withUpdateExpression("set #name = :newValue")
                .withConditionExpression("#name = :currentValue")
                .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute(SOME_FIELD_VALUE
                    + TABLE3 + org))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalFailHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE3)
                    .withKey(buildHkRkKey(testArgument.getHashKeyAttrType()))
                    .withUpdateExpression("set #name = :newValue")
                    .withConditionExpression("#name = :currentValue")
                    .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                    .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
                    .addExpressionAttributeValuesEntry(":newValue", createStringAttribute(SOME_FIELD_VALUE
                        + TABLE3 + org + "Updated")));
                throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertTrue(e.getMessage().contains("ConditionalCheckFailedException"));
            }
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE3 + org)));
        });
    }

}