package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_RK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
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
    void updatePrimaryKeyFieldFails(TestArgument testArgument) {
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

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateConditionalOnHkSuccessWithLiterals(TestArgument testArgument) {
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
            assertEquals(ItemBuilder
                    .builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org + "Updated")
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
    void updateConditionalOnGsiRkSuccess_usePlaceholder(TestArgument testArgument) {
        runUpdateConditionalOnGsiRkTest(testArgument, true);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
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
                    ":newValue", createStringAttribute(INDEX_FIELD_VALUE + TABLE3 + org + "Updated"))
                .addExpressionAttributeValuesEntry(
                    ":currentValue", createStringAttribute(INDEX_FIELD_VALUE));
            if (usePlaceHolder) {
                updateItemRequest.withUpdateExpression("set #indexField = :newValue")
                    .withConditionExpression("#indexField = :currentValue")
                    .addExpressionAttributeNamesEntry("#indexField", INDEX_FIELD);
            } else {
                updateItemRequest.withUpdateExpression("set " + INDEX_FIELD + " = :newValue")
                    .withConditionExpression(INDEX_FIELD + " = :currentValue");
            }
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);

            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                    .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                    .indexField(S, INDEX_FIELD_VALUE + TABLE3 + org + "Updated")
                    .gsiHkField(S, GSI_HK_FIELD_VALUE)
                    .gsi2HkField(S, GSI2_HK_FIELD_VALUE)
                    .gsi2RkField(N, GSI2_RK_FIELD_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_OTHER_S_VALUE)));
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
    void updateHkRkTable(TestArgument testArgument) {
        runUpdateHkRkTableTest(testArgument, TABLE3);
    }

    @ParameterizedTest
    @ArgumentsSource(DefaultArgumentProvider.class)
    void updateHkRkTableWithTableRkInGsi(TestArgument testArgument) {
        runUpdateHkRkTableTest(testArgument, TABLE5);
    }

    private void runUpdateHkRkTableTest(TestArgument testArgument, String tableName) {
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> updateItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                HASH_KEY_VALUE)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build();

            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(updateItemKey)
                .withUpdateExpression("set #indexField = :indexValue, #someField = :someValue")
                .withExpressionAttributeNames(ImmutableMap.of(
                    "#indexField", INDEX_FIELD,
                    "#someField", SOME_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(
                    ":indexValue", createStringAttribute(INDEX_FIELD_VALUE + tableName + org + "Updated"),
                    ":someValue", createStringAttribute(SOME_FIELD_VALUE + tableName + org + "Updated")));
            testArgument.getAmazonDynamoDb().updateItem(updateItemRequest);
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + tableName + org + "Updated")
                    .indexField(S, INDEX_FIELD_VALUE + tableName + org + "Updated")
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
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
    void updateFieldInMultipleGsis(TestArgument testArgument) {
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
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
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
            assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE3 + org + "Updated")
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



}
