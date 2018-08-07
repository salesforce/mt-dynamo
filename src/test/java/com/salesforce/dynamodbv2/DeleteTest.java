package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.TestSetup.TABLE2;
import static com.salesforce.dynamodbv2.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkKey;
import static com.salesforce.dynamodbv2.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildKey;
import static com.salesforce.dynamodbv2.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests deleteItem().
 *
 * @author msgroi
 */
class DeleteTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void delete(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        Map<String, AttributeValue> deleteItemKey = buildKey(testArgument.getHashKeyAttrType());
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(TABLE1).withKey(deleteItemKey);
        testArgument.getAmazonDynamoDb().deleteItem(deleteItemRequest);
        assertNull(getItem(testArgument.getHashKeyAttrType(),
                   testArgument.getAmazonDynamoDb(), TABLE1, HASH_KEY_VALUE));
        assertEquals(TABLE1, deleteItemRequest.getTableName()); // assert no side effects
        assertThat(deleteItemRequest.getKey(), is(originalDeleteItemKey)); // assert no side effects
        assertThat(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                                        SOME_FIELD_VALUE + TABLE2 + org),
                   is(getItem(testArgument.getHashKeyAttrType(),
                              testArgument.getAmazonDynamoDb(),
                              TABLE2))); // assert different table, same org
        testArgument.getOrgs().stream().filter(otherOrg -> !otherOrg.equals(org)).forEach(otherOrg -> {
            MT_CONTEXT.setContext(otherOrg);
            // assert same table, different orgs
            assertThat(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                                             SOME_FIELD_VALUE + TABLE1 + otherOrg),
                       is(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1)));
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void deleteHkRkTable(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        Map<String, AttributeValue> deleteItemKey = buildHkRkKey(testArgument.getHashKeyAttrType());
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(TABLE3).withKey(deleteItemKey);
        testArgument.getAmazonDynamoDb().deleteItem(deleteItemRequest);
        assertNull(getItem(testArgument.getHashKeyAttrType(),
            testArgument.getAmazonDynamoDb(), TABLE3, HASH_KEY_VALUE, Optional.of(RANGE_KEY_VALUE)));
        assertEquals(TABLE3, deleteItemRequest.getTableName()); // assert no side effects
        assertThat(deleteItemRequest.getKey(), is(originalDeleteItemKey)); // assert no side effects
        testArgument.getOrgs().stream().filter(otherOrg -> !otherOrg.equals(org)).forEach(otherOrg -> {
            MT_CONTEXT.setContext(otherOrg);
            // assert same table, different orgs
            assertThat(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                                                 SOME_FIELD_VALUE + TABLE3 + otherOrg),
                       is(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3)));
        });
    }

}