package com.salesforce.dynamodbv2.parameterizedtests;

import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE2;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkKey;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildKey;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
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
 * Tests deleteItem().
 *
 * @author msgroi
 */
@Tag("parameterized-tests")
class ParameterizedDeleteTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
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

    @ParameterizedTest
    @ArgumentsSource(ParameterizedTestArgumentProvider.class)
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