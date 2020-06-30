package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.AmazonDynamoDbStrategy.HashPartitioning;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_RK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_OTHER_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_RK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_OTHER_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createNumberAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
import com.salesforce.dynamodbv2.testsupport.DefaultTestSetup;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void setUpdate(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("set #someField = :someValue")
                .withExpressionAttributeNames(ImmutableMap.of("#someField", SOME_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":someValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
            assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
            assertEquals(TABLE1, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void addUpdate(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("add #someOtherField :" + SOME_OTHER_FIELD_VALUE)
                .withExpressionAttributeNames(ImmutableMap.of("#someOtherField","someOtherField"))
                .withExpressionAttributeValues(ImmutableMap.of(":" + SOME_OTHER_FIELD_VALUE,
                    createNumberAttribute("1")));

            // assert add works for attribute_not_exists, and for attribute_exists (increments value by 1)
            String[] expectedValues = new String[]{"1", "2"};
            for (String expectedValue : expectedValues) {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .someOtherField(N, expectedValue)
                        .build(),
                    getItem(testArgument.getAmazonDynamoDb(),
                        TABLE1,
                        HASH_KEY_VALUE,
                        testArgument.getHashKeyAttrType(),
                        Optional.empty()));

                assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
                assertEquals(TABLE1, updateItemRequest.getTableName()); // assert no side effects
            }
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void setAndAddUpdate(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("set #someField = :someValue add #someOtherField :" + SOME_OTHER_FIELD_VALUE)
                .withExpressionAttributeNames(ImmutableMap.of("#someField", SOME_FIELD, "#someOtherField",
                    "someOtherField"))
                .withExpressionAttributeValues(ImmutableMap.of(":someValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated"), ":"
                        + SOME_OTHER_FIELD_VALUE, createNumberAttribute("1")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .someOtherField(N, "1")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
            assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
            assertEquals(TABLE1, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updatePrimaryKeyFieldFailsUsingSet(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("set #someField = :someValue")
                .withExpressionAttributeNames(ImmutableMap.of("#someField", HASH_KEY_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":someValue",
                    createStringAttribute("someUpdatedValue")));
            try {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                fail("Updating table PK field should fail");
            } catch (AmazonServiceException e) {
                assertTrue(e.getMessage().contains("This attribute is part of the key"));
            }
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updatePrimaryKeyFieldFailsUsingAdd(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();

            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("add #someField :someValue")
                .withExpressionAttributeNames(ImmutableMap.of("#someField", HASH_KEY_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":someValue", createNumberAttribute("1")));
            try {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                fail("Updating table PK field should fail");
            } catch (AmazonServiceException e) {
                assertTrue(e.getMessage().contains("This attribute is part of the key"));
            }
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateSetFieldMultipleTimesFails(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("set #someField = :someValue, " + SOME_FIELD + " = :someValue")
                .withExpressionAttributeNames(ImmutableMap.of("#someField", SOME_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":someValue",
                    createStringAttribute("someUpdatedValue")));
            try {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                fail("Update expression with the same field being set more than once should fail");
            } catch (AmazonServiceException e) {
                assertTrue(e.getMessage().contains("Two document paths overlap with each other"));
            }
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateAddFieldMultipleTimesFails(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .build();

            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(updateItemKey)
                .withUpdateExpression("add someField :someValue, " + SOME_FIELD + " :someValue")
                .withExpressionAttributeValues(ImmutableMap.of(":someValue", createNumberAttribute("1")));
            try {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                fail("Update expression with the same field being added to more than once should fail");
            } catch (AmazonServiceException e) {
                assertTrue(e.getMessage().contains("Two document paths overlap with each other"));
            }
        });
    }

    /**
     * The test ensures that if a field name appears in a conditional expression for an update expression
     * containing set, the conditional expression still works as expected.
     * @param testArgument argument for testing.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateSetConditionalSuccess(TestArgument testArgument) {
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
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    /**
     * The test ensures that if a field name appears in a conditional expression for an update expression
     * containing add, the conditional expression still works as expected.
     * @param testArgument argument for testing.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateAddConditionalSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .build())
                .withUpdateExpression("add #someOtherField :newValue")
                .withConditionExpression("attribute_not_exists(#someOtherField)")
                .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                .addExpressionAttributeValuesEntry(":newValue", createNumberAttribute("1")));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .someOtherField(N, "1")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    /**
     * The test ensures that if a field name appears in a conditional expression for an update expression
     * containing set and add, the conditional expression still works as expected.
     * @param testArgument argument for testing.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateSetAndAddConditionalSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .build())
                .withUpdateExpression("set #someField = :newValue add #someOtherField :addValue")
                .withConditionExpression("#someField = :currentValue and attribute_not_exists(#someOtherField)")
                .withExpressionAttributeNames(ImmutableMap.of("#someField", SOME_FIELD, "#someOtherField",
                    SOME_OTHER_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(
                    ":currentValue", createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org),
                    ":newValue", createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated"),
                    ":addValue", createNumberAttribute("1")
                )));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .someOtherField(N, "1")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateConditionalOnHkSuccess(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .build())
                .withUpdateExpression("set #someField = :newValue add #someOtherField :addValue")
                .withConditionExpression("#hk = :currentValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeNamesEntry("#hk", HASH_KEY_FIELD)
                .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated"))
                .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .someOtherField(N, "1")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateConditionalOnHkSuccessWithLiterals(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE1)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .build())
                .withUpdateExpression("set " + SOME_FIELD + " = :newValue add " + SOME_OTHER_FIELD + ":addValue")
                .withConditionExpression(HASH_KEY_FIELD + " = :currentValue")
                .addExpressionAttributeValuesEntry(":currentValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated"))
                .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
            assertEquals(ItemBuilder
                    .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
                    .someOtherField(N, "1")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateConditionalOnGsiRkSuccess_usePlaceholder(TestArgument testArgument) {
        runUpdateConditionalOnGsiRkTest(testArgument, true);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateConditionalOnGsiRkSuccess_useLiteral(TestArgument testArgument) {
        runUpdateConditionalOnGsiRkTest(testArgument, false);
    }

    private void runUpdateConditionalOnGsiRkTest(TestArgument testArgument, boolean usePlaceHolder) {
        testArgument.forEachOrgContext(org -> {
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE).build())
                .addExpressionAttributeValuesEntry(
                    ":hkValue", createAttributeValue(S, GSI2_HK_FIELD_VALUE + TABLE3 + org + "Updated"))
                .addExpressionAttributeValuesEntry(
                    ":rkValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE + "999"))
                .addExpressionAttributeValuesEntry(
                    ":currentValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE));
            if (usePlaceHolder) {
                updateItemRequest.withUpdateExpression("set #hkField = :hkValue, #rkField = :rkValue")
                    .withConditionExpression("#rkField = :currentValue")
                    .addExpressionAttributeNamesEntry("#hkField", GSI2_HK_FIELD)
                    .addExpressionAttributeNamesEntry("#rkField", GSI2_RK_FIELD);
            } else {
                updateItemRequest.withUpdateExpression("set " + GSI2_HK_FIELD + " = :hkValue, "
                    + GSI2_RK_FIELD + " = :rkValue")
                    .withConditionExpression(GSI2_RK_FIELD + " = :currentValue");
            }
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                    .indexField(S, INDEX_FIELD_VALUE)
                    .gsiHkField(S, GSI_HK_FIELD_VALUE)
                    .gsi2HkField(S, GSI2_HK_FIELD_VALUE + TABLE3 + org + "Updated")
                    .gsi2RkField(N, GSI2_RK_FIELD_VALUE + "999")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_OTHER_S_VALUE)));
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateWithAddConditionalOnGsiRkSuccess_usePlaceholder(TestArgument testArgument) {
        runUpdateWithAddConditionalOnGsiRkTest(testArgument, true);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateWithAddConditionalOnGsiRkSuccess_useLiteral(TestArgument testArgument) {
        runUpdateWithAddConditionalOnGsiRkTest(testArgument, false);
    }

    private void runUpdateWithAddConditionalOnGsiRkTest(TestArgument testArgument, boolean usePlaceHolder) {
        testArgument.forEachOrgContext(org -> {
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE).build())
                .addExpressionAttributeValuesEntry(
                    ":hkValue", createAttributeValue(S, GSI2_HK_FIELD_VALUE + TABLE3 + org + "Updated"))
                .addExpressionAttributeValuesEntry(
                    ":rkValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE + "999"))
                .addExpressionAttributeValuesEntry(
                    ":currentValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE));
            if (usePlaceHolder) {
                updateItemRequest.withUpdateExpression("set #hkField = :hkValue, #rkField = :rkValue")
                    .withConditionExpression("#rkField = :currentValue")
                    .addExpressionAttributeNamesEntry("#hkField", GSI2_HK_FIELD)
                    .addExpressionAttributeNamesEntry("#rkField", GSI2_RK_FIELD);
            } else {
                updateItemRequest.withUpdateExpression("set " + GSI2_HK_FIELD + " = :hkValue, "
                    + GSI2_RK_FIELD + " = :rkValue")
                    .withConditionExpression(GSI2_RK_FIELD + " = :currentValue");
            }
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                    .indexField(S, INDEX_FIELD_VALUE)
                    .gsiHkField(S, GSI_HK_FIELD_VALUE)
                    .gsi2HkField(S, GSI2_HK_FIELD_VALUE + TABLE3 + org + "Updated")
                    .gsi2RkField(N, GSI2_RK_FIELD_VALUE + "999")
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_OTHER_S_VALUE)));

            // attempt to update a secondary index which is part of the index key
            updateItemRequest = new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE).build())
                .addExpressionAttributeValuesEntry(
                    ":rkValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE + "999"))
                .addExpressionAttributeValuesEntry(
                    ":updateValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE));

            if (usePlaceHolder) {
                updateItemRequest.withUpdateExpression("add #rkField :updateValue")
                    .withConditionExpression("#rkField = :rkValue")
                    .addExpressionAttributeNamesEntry("#rkField", GSI2_RK_FIELD);
            } else {
                updateItemRequest.withUpdateExpression("add " + GSI2_RK_FIELD + " :updateValue")
                    .withConditionExpression(GSI2_RK_FIELD + " = :rkValue");
            }

            try {
                testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
                if (testArgument.getAmazonDynamoDbStrategy().equals(HashPartitioning)) {
                    fail("Update expression containing add with an index field should fail");
                }
            } catch (AmazonServiceException e) {
                assertTrue(e.getMessage().contains("This secondary index is part of the index key"));
            }

            String expectedNewValue = String.valueOf(Integer.parseInt(GSI2_RK_FIELD_VALUE + "999") + 2);

            ItemBuilder itemBuilder = ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                .indexField(S, INDEX_FIELD_VALUE)
                .gsiHkField(S, GSI_HK_FIELD_VALUE)
                .gsi2HkField(S, GSI2_HK_FIELD_VALUE + TABLE3 + org + "Updated");

            if (testArgument.getAmazonDynamoDbStrategy().equals(HashPartitioning)) {
                itemBuilder.gsi2RkField(N, GSI2_RK_FIELD_VALUE + "999");
            } else {
                itemBuilder.gsi2RkField(N, expectedNewValue);
            }

            assertEquals(itemBuilder.build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_OTHER_S_VALUE)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateConditionalFail(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE1)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
                    .withUpdateExpression("set #name = :newValue add #someOtherField :addValue")
                    .withConditionExpression("#name = :currentValue and attribute_exists(#someOtherField)")
                    .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                    .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                    .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
                    .addExpressionAttributeValuesEntry(":newValue",
                        createStringAttribute(SOME_FIELD_VALUE + TABLE1 + org + "Updated"))
                    .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
                throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertTrue(e.getMessage().contains("ConditionalCheckFailedException"));
            }
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty()));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateHkRkTable(TestArgument testArgument) {
        runUpdateHkRkTableTest(testArgument, TABLE3, ImmutableMap.of(
            GSI_HK_FIELD, createStringAttribute(GSI_HK_FIELD_VALUE + "Updated")));
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE5 })
    void updateHkRkTableWithTableRkInGsi(TestArgument testArgument) {
        runUpdateHkRkTableTest(testArgument, TABLE5, ImmutableMap.of(
            GSI_HK_FIELD, createStringAttribute(GSI_HK_FIELD_VALUE + "Updated"),
            INDEX_FIELD, createStringAttribute(INDEX_FIELD_VALUE + "Updated"),
            GSI2_RK_FIELD, createAttributeValue(N, GSI2_RK_FIELD_VALUE + "999")));
    }

    private void runUpdateHkRkTableTest(TestArgument testArgument, String tableName,
                                        Map<String, AttributeValue> gsiFieldUpdates) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build();

            Map<String, AttributeValue> fieldUpdates = new HashMap<>(gsiFieldUpdates);
            fieldUpdates.put(SOME_FIELD, createStringAttribute(SOME_FIELD_VALUE + tableName + org + "Updated"));

            List<String> setClauses = new ArrayList<>(fieldUpdates.size());
            Map<String, AttributeValue> valuePlaceholders = new HashMap<>();
            fieldUpdates.forEach((field, value) -> {
                String placeholder = ":" + field + "Value";
                valuePlaceholders.put(placeholder, value);
                setClauses.add(field + " = " + placeholder);
            });
            String updateExpression = "SET " + String.join(", ", setClauses);

            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(updateItemKey)
                .withUpdateExpression(updateExpression)
                .withExpressionAttributeValues(valuePlaceholders);
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            Map<String, AttributeValue> expectedItem = DefaultTestSetup.getSimpleItemWithStringRk(
                testArgument.getHashKeyAttrType(), tableName, org)
                .putAll(fieldUpdates)
                .build();
            assertEquals(expectedItem,
                getItem(testArgument.getAmazonDynamoDb(),
                    tableName,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
            assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
            assertEquals(tableName, updateItemRequest.getTableName()); // assert no side effects
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateGsiFieldAndQueryGsi(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String tableName = TABLE3;

            QueryRequest queryGsiRequest = new QueryRequest().withTableName(tableName)
                .withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", GSI_HK_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createStringAttribute(GSI_HK_FIELD_VALUE)))
                .withIndexName("testGsi");
            assertEquals(1, testArgument.getAmazonDynamoDb().query(queryGsiRequest).getItems().size());

            Map<String, AttributeValue> updateItemKey = ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(updateItemKey)
                .withUpdateExpression("set #gsiHkField = :gsiHkValue")
                .withExpressionAttributeNames(ImmutableMap.of("#gsiHkField", GSI_HK_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(
                    ":gsiHkValue", createStringAttribute(GSI_HK_FIELD_VALUE)));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            assertEquals(
                ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + tableName + org)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .gsiHkField(S, GSI_HK_FIELD_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    tableName,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
            assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
            assertEquals(tableName, updateItemRequest.getTableName()); // assert no side effects

            // query the record based on index
            assertEquals(2, testArgument.getAmazonDynamoDb().query(queryGsiRequest).getItems().size());
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE5 })
    void updateFieldInMultipleGsis(TestArgument testArgument) {
        // skip if using hash partitioning, which doesn't allow updating only 1 but not all fields of a secondary index
        if (testArgument.getAmazonDynamoDbStrategy() == HashPartitioning) {
            return;
        }

        testArgument.forEachOrgContext(org -> {
            String tableName = TABLE5;

            QueryRequest queryGsiRequest = new QueryRequest().withTableName(tableName)
                .withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", GSI_HK_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createStringAttribute(GSI_HK_FIELD_VALUE)));
            assertEquals(1, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_1")).getItems().size());
            assertEquals(1, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_2")).getItems().size());

            Map<String, AttributeValue> updateItemKey = ItemBuilder
                .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build();
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(updateItemKey)
                .withUpdateExpression("SET #gsiHkField = :gsiHkValue")
                .withExpressionAttributeNames(ImmutableMap.of("#gsiHkField", GSI_HK_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(
                    ":gsiHkValue", createStringAttribute(GSI_HK_FIELD_VALUE)));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            ItemBuilder expectedItemBuilder = ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + tableName + org)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .gsiHkField(S, GSI_HK_FIELD_VALUE);
            assertEquals(expectedItemBuilder.build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    tableName,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
            assertEquals(new HashMap<>(updateItemKey), updateItemRequest.getKey()); // assert no side effects
            assertEquals(tableName, updateItemRequest.getTableName()); // assert no side effects

            assertEquals(1, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_1")).getItems().size());
            assertEquals(1, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_2")).getItems().size());

            updateItemRequest
                .withUpdateExpression("SET #indexField = :indexFieldValue, #gsi2RkField = :gsi2RkFieldValue")
                .withExpressionAttributeNames(ImmutableMap.of(
                    "#indexField", INDEX_FIELD,
                    "#gsi2RkField", GSI2_RK_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(
                    ":indexFieldValue", createStringAttribute(INDEX_FIELD_VALUE + "other"),
                    ":gsi2RkFieldValue", createAttributeValue(N, GSI2_RK_FIELD_VALUE + "2")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            expectedItemBuilder.indexField(S, INDEX_FIELD_VALUE + "other");
            expectedItemBuilder.gsi2RkField(N, GSI2_RK_FIELD_VALUE + "2");
            assertEquals(expectedItemBuilder.build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    tableName,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));

            assertEquals(2, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_1")).getItems().size());
            assertEquals(2, testArgument.getAmazonDynamoDb().query(
                queryGsiRequest.withIndexName("testGsi_on_common_field_2")).getItems().size());
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateConditionalSuccessHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build())
                .withUpdateExpression("set #someField = :newValue add #someOtherField :addValue")
                .withConditionExpression("#someField = :currentValue and attribute_not_exists(#someOtherField)")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute(SOME_FIELD_VALUE
                    + TABLE3 + org))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated"))
                .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
                    .someOtherField(N, "1")
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateConditionalOnHkRkSuccessHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build())
                .withUpdateExpression("set #someField = :newValue add #someOtherField :addValue")
                .withConditionExpression("#hk = :currentHkValue and #rk = :currentRkValue")
                .addExpressionAttributeNamesEntry("#someField", SOME_FIELD)
                .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                .addExpressionAttributeNamesEntry("#hk", HASH_KEY_FIELD)
                .addExpressionAttributeNamesEntry("#rk", RANGE_KEY_FIELD)
                .addExpressionAttributeValuesEntry(":currentHkValue",
                    createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE))
                .addExpressionAttributeValuesEntry(":currentRkValue", createStringAttribute(RANGE_KEY_S_VALUE))
                .addExpressionAttributeValuesEntry(":newValue",
                    createStringAttribute(SOME_FIELD_VALUE + TABLE3 + org + "Updated"))
                .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
                    .someOtherField(N, "1")
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateConditionalFailHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE3)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .rangeKey(S, RANGE_KEY_S_VALUE)
                        .build())
                    .withUpdateExpression("set #name = :newValue add #someOtherField :addValue")
                    .withConditionExpression("#name = :currentValue and attribute_exists(#someOtherField)")
                    .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
                    .addExpressionAttributeNamesEntry("#someOtherField", SOME_OTHER_FIELD)
                    .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
                    .addExpressionAttributeValuesEntry(":newValue", createStringAttribute(SOME_FIELD_VALUE
                        + TABLE3 + org + "Updated"))
                    .addExpressionAttributeValuesEntry(":addValue", createNumberAttribute("1")));
                throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
            } catch (ConditionalCheckFailedException e) {
                assertTrue(e.getMessage().contains("ConditionalCheckFailedException"));
            }
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void attributeUpdatesNotSupportedInSharedTable(TestArgument testArgument) {
        if (testArgument.getAmazonDynamoDb() instanceof MtAmazonDynamoDbBySharedTable) {
            try {
                testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                    .withTableName(TABLE1)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE).build())
                    .addAttributeUpdatesEntry(SOME_FIELD,
                        new AttributeValueUpdate()
                            .withValue(createStringAttribute(SOME_FIELD_VALUE + TABLE1 + "Updated"))));
                fail("expected IllegalArgumentException not encountered");
            } catch (IllegalArgumentException e) {
                assertEquals(
                    "Use of attributeUpdates in UpdateItemRequest objects is not supported.  "
                        + "Use UpdateExpression instead.",
                    e.getMessage());
            }
        }
    }

    /**
     * Calling UpdateItem without an update expression on an existing key should end up not affecting the specified
     * record.
     */
    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE1 })
    void updateWithoutUpdateExpression(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> key = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE).build();
            Map<String, AttributeValue> existingItem = getItem(testArgument.getAmazonDynamoDb(),
                TABLE1, HASH_KEY_VALUE, testArgument.getHashKeyAttrType(), Optional.empty());

            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest().withTableName(TABLE1).withKey(key));

            Map<String, AttributeValue> item = getItem(testArgument.getAmazonDynamoDb(),
                TABLE1, HASH_KEY_VALUE, testArgument.getHashKeyAttrType(), Optional.empty());
            assertEquals(existingItem, item);
        });
    }

    /**
     * Calling UpdateItem with a key that doesn't yet exist and no update expression should result in creating a new
     * record with the specified key.
     */
    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateNewKeyWithoutUpdateExpression(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> key = ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .rangeKey(S, "abcde").build();
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(HASH_KEY_FIELD + " = :v1 AND " + RANGE_KEY_FIELD + " = :v2")
                .withExpressionAttributeValues(ImmutableMap.of(":v1", createAttributeValue(
                    testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                    ":v2", createAttributeValue(S, "abcde")));
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(0, items.size());

            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest().withTableName(TABLE3).withKey(key));

            Map<String, AttributeValue> item = getItem(testArgument.getAmazonDynamoDb(),
                TABLE3, HASH_KEY_VALUE, testArgument.getHashKeyAttrType(), Optional.of("abcde"));
            assertEquals(key, item);
        });
    }

    /**
     * Calling UpdateItem with a key that doesn't yet exist and an update expression on some non-key field should
     * result in creating a new record with the specified key and the specified non-key field value.
     */
    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = { TABLE3 })
    void updateNewKey(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> key = ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .rangeKey(S, "abcde").build();
            QueryRequest queryRequest = new QueryRequest().withTableName(TABLE3)
                .withKeyConditionExpression(HASH_KEY_FIELD + " = :v1 AND " + RANGE_KEY_FIELD + " = :v2")
                .withExpressionAttributeValues(ImmutableMap.of(":v1", createAttributeValue(
                    testArgument.getHashKeyAttrType(), HASH_KEY_VALUE),
                    ":v2", createAttributeValue(S, "abcde")));
            List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb().query(queryRequest).getItems();
            assertEquals(0, items.size());

            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(TABLE3)
                .withKey(key)
                .withUpdateExpression("SET " + SOME_FIELD + " = :value")
                .addExpressionAttributeValuesEntry(":value", createStringAttribute("someValue")));

            Map<String, AttributeValue> item = getItem(testArgument.getAmazonDynamoDb(),
                TABLE3, HASH_KEY_VALUE, testArgument.getHashKeyAttrType(), Optional.of("abcde"));
            Map<String, AttributeValue> expectedItem = new HashMap<>(key);
            expectedItem.put(SOME_FIELD, createStringAttribute("someValue"));
            assertEquals(expectedItem, item);
        });
    }

}
