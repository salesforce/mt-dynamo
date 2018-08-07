package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.TestSupport.SOME_FIELD;
import static com.salesforce.dynamodbv2.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkKey;
import static com.salesforce.dynamodbv2.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildKey;
import static com.salesforce.dynamodbv2.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author msgroi
 */
class UpdateTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void update(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            Map<String, AttributeValue> updateItemKey = buildKey(testArgument.getHashKeyAttrType());
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate().withValue(createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            testArgument.getAmazonDynamoDB().updateItem(updateItemRequest);
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(updateItemRequest.getKey(), is(new HashMap<>(updateItemKey))); // assert no side effects
            assertEquals(TABLE1, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void updateConditionalSuccess(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            testArgument.getAmazonDynamoDB().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(buildKey(testArgument.getHashKeyAttrType()))
                .withUpdateExpression("set #name = :newValue")
                .withConditionExpression("#name = :currentValue")
                .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute(SOME_FIELD_VALUE
                    + TABLE1 + org))
                .addExpressionAttributeValuesEntry(":newValue", createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void updateConditionalFail(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            try {
                testArgument.getAmazonDynamoDB().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE1)
                    .withKey(buildKey(testArgument.getHashKeyAttrType()))
                    .withUpdateExpression("set #name = :newValue")
                    .withConditionExpression("#name = :currentValue")
                    .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                    .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
                    .addExpressionAttributeValuesEntry(":newValue", createStringAttribute(SOME_FIELD_VALUE
                        + TABLE1 + org + "Updated")));
                throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertTrue(e.getMessage().contains("ConditionalCheckFailedException"));
            }
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void updateHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            Map<String, AttributeValue> updateItemKey = buildHkRkKey(testArgument.getHashKeyAttrType());
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate().withValue(createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            testArgument.getAmazonDynamoDB().updateItem(updateItemRequest);
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(updateItemRequest.getKey(), is(new HashMap<>(updateItemKey))); // assert no side effects
            assertEquals(TABLE3, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void updateConditionalSuccessHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            testArgument.getAmazonDynamoDB().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(buildHkRkKey(testArgument.getHashKeyAttrType()))
                .withUpdateExpression("set #name = :newValue")
                .withConditionExpression("#name = :currentValue")
                .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute(SOME_FIELD_VALUE
                    + TABLE3 + org))
                .addExpressionAttributeValuesEntry(":newValue", createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void updateConditionalFailHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            try {
                testArgument.getAmazonDynamoDB().updateItem(new UpdateItemRequest()
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
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDB(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(), SOME_FIELD_VALUE + TABLE3 + org)));
        });
    }

}