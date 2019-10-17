package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE2;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
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
 * Tests deleteItem().
 *
 * @author msgroi
 */
class DeleteTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = ArgumentBuilder.MT_CONTEXT;

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1, TABLE2})
    void delete(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        Map<String, AttributeValue> deleteItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                    HASH_KEY_VALUE)
                .build();
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(TABLE1).withKey(deleteItemKey);
        testArgument.getAmazonDynamoDb().deleteItem(deleteItemRequest);
        assertNull(getItem(testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.empty()));
        assertEquals(TABLE1, deleteItemRequest.getTableName()); // assert no side effects
        assertEquals(originalDeleteItemKey, deleteItemRequest.getKey()); // assert no side effects
        assertEquals(getItem(testArgument.getAmazonDynamoDb(),
            TABLE2,
            HASH_KEY_VALUE,
            testArgument.getHashKeyAttrType(),
            Optional.empty()),
            ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE2 + org)
                .build()); // assert different table, same org
        testArgument.getOrgs().stream().filter(otherOrg -> !otherOrg.equals(org)).forEach(otherOrg -> {
            MT_CONTEXT.setContext(otherOrg);
            // assert same table, different orgs
            assertEquals(getItem(testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.empty()),
                ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + otherOrg)
                    .build());
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void deleteWithAttributeExists(TestArgument testArgument) {
        MT_CONTEXT.setContext(testArgument.getOrgs().get(0));
        testArgument.getAmazonDynamoDb().deleteItem(new DeleteItemRequest()
            .withTableName(TABLE1).withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
            .withConditionExpression("attribute_exists(#field)")
            .withExpressionAttributeNames(ImmutableMap.of("#field", SOME_FIELD)));
        assertNull(getItem(testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.empty()));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void deleteWithAttributeExistsWithLiterals(TestArgument testArgument) {
        MT_CONTEXT.setContext(testArgument.getOrgs().get(0));
        testArgument.getAmazonDynamoDb().deleteItem(new DeleteItemRequest()
            .withTableName(TABLE1).withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
            .withConditionExpression("attribute_exists(" + SOME_FIELD + ")"));
        assertNull(getItem(testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.empty()));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void deleteWithAttributeExistsFails(TestArgument testArgument) {
        MT_CONTEXT.setContext(testArgument.getOrgs().get(0));
        try {
            testArgument.getAmazonDynamoDb().deleteItem(new DeleteItemRequest()
                    .withTableName(TABLE1)
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .build())
                .withConditionExpression("attribute_exists(#field)")
                .withExpressionAttributeNames(ImmutableMap.of("#field", "someNonExistentField")));
            fail("expected ConditionalCheckFailedException not encountered");
        } catch (ConditionalCheckFailedException ignore) {
            // expected
        }
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void deleteWithHkAttributeExists(TestArgument testArgument) {
        MT_CONTEXT.setContext(testArgument.getOrgs().get(0));
        testArgument.getAmazonDynamoDb().deleteItem(new DeleteItemRequest()
            .withTableName(TABLE1).withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
            .withConditionExpression("attribute_exists(#field)")
            .withExpressionAttributeNames(ImmutableMap.of("#field", HASH_KEY_FIELD)));
        assertNull(getItem(testArgument.getAmazonDynamoDb(),
                TABLE1,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.empty()));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE3})
    void deleteHkRkTable(TestArgument testArgument) {
        runDeleteTestForHkRkTable(testArgument, TABLE3);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE5})
    void deleteHkRkTableWithPkFieldInGsi(TestArgument testArgument) {
        runDeleteTestForHkRkTable(testArgument, TABLE5);
    }

    private void runDeleteTestForHkRkTable(TestArgument testArgument, String tableName) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        Map<String, AttributeValue> deleteItemKey = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
            HASH_KEY_VALUE)
            .rangeKey(S, RANGE_KEY_S_VALUE)
            .build();
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName).withKey(deleteItemKey);
        testArgument.getAmazonDynamoDb().deleteItem(deleteItemRequest);
        assertNull(getItem(testArgument.getAmazonDynamoDb(),
            tableName,
            HASH_KEY_VALUE,
            testArgument.getHashKeyAttrType(),
            Optional.of(RANGE_KEY_S_VALUE)));
        assertEquals(tableName, deleteItemRequest.getTableName()); // assert no side effects
        assertEquals(originalDeleteItemKey, deleteItemRequest.getKey()); // assert no side effects
        testArgument.getOrgs().stream().filter(otherOrg -> !otherOrg.equals(org)).forEach(otherOrg -> {
            MT_CONTEXT.setContext(otherOrg);
            // assert same table, different orgs
            assertEquals(getItem(testArgument.getAmazonDynamoDb(),
                tableName,
                HASH_KEY_VALUE,
                testArgument.getHashKeyAttrType(),
                Optional.of(RANGE_KEY_S_VALUE)),
                ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + tableName + otherOrg)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build());
        });
    }
}
