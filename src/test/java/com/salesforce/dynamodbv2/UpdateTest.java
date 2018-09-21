package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
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
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                        HASH_KEY_VALUE)
                    .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate()
                        .withValue(createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                    is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                            .build()));
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
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
                .withUpdateExpression("set #someField = :newValue")
                .withConditionExpression("#someField = :currentValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                        .build()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalOnHkSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
                .withUpdateExpression("set #someField = :newValue")
                .withConditionExpression("#hk = :currentValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeNamesEntry("#hk", HASH_KEY_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                        .build()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalOnHkWithLiteralsSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
                .withUpdateExpression("set " + SOME_FIELD + " = :newValue")
                .withConditionExpression(HASH_KEY_FIELD + " = :currentValue")
                .addExpressionAttributeValuesEntry(":currentValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                        .build()));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalFail(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE1)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .build())
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
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()),
                       is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                               .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                               .build()));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                    HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate().withValue(
                        createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)),
                       is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                               .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
                               .rangeKey(S, RANGE_KEY_S_VALUE)
                               .build()));
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
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .rangeKey(S, RANGE_KEY_S_VALUE)
                        .build())
                .withUpdateExpression("set #someField = :newValue")
                .withConditionExpression("#someField = :currentValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute(SOME_FIELD_VALUE
                    + TABLE3 + org))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)),
                       is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                               .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
                               .rangeKey(S, RANGE_KEY_S_VALUE)
                               .build()));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalOnHkRkSuccessHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .rangeKey(S, RANGE_KEY_S_VALUE)
                        .build())
                .withUpdateExpression("set #someField = :newValue")
                .withConditionExpression("#hk = :currentHkValue and #rk = :currentRkValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeNamesEntry("#hk", HASH_KEY_FIELD)
                .addExpressionAttributeNamesEntry("#rk", RANGE_KEY_FIELD)
                .addExpressionAttributeValuesEntry(":currentHkValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":currentRkValue", createStringAttribute(RANGE_KEY_S_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated")));
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)),
                is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
                        .rangeKey(S, RANGE_KEY_S_VALUE)
                        .build()));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalFailHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE3)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .rangeKey(S, RANGE_KEY_S_VALUE)
                            .build())
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
            assertThat(getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)),
                       is(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                               .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                               .rangeKey(S, RANGE_KEY_S_VALUE)
                               .build()));
        });
    }

}
