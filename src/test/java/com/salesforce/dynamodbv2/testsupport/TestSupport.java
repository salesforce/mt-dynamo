package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper methods for building, getting, and doing type conversions that are frequently necessary in unit tests.
 *
 * @author msgroi
 */
public class TestSupport {

    public static final boolean IS_LOCAL_DYNAMO = true;
    public static final int TIMEOUT_SECONDS = 60;
    // we use a number for HASH_KEY_VALUE since it nicely works with each of Dynamo's scalar-attribute types (S, N, B)
    public static final String HASH_KEY_VALUE = "1";
    public static final String HASH_KEY_OTHER_VALUE = "2";
    public static final String RANGE_KEY_VALUE = "rangeKeyValue";
    public static final String RANGE_KEY_OTHER_VALUE = RANGE_KEY_VALUE + "2";
    public static final String SOME_FIELD_VALUE = "someValue";
    public static final String SOME_OTHER_FIELD_VALUE = SOME_FIELD_VALUE + "2";
    public static final String SOME_OTHER_OTHER_FIELD_VALUE = "someOtherValue";
    public static final String INDEX_FIELD_VALUE = "indexFieldValue";

    /**
     * Retrieves the item with the provided HK and RK values.
     */
    public static Map<String, AttributeValue> getItem(AmazonDynamoDB amazonDynamoDb,
                                                      String tableName,
                                                      String hashKeyValue,
                                                      ScalarAttributeType hashKeyAttrType,
                                                      Optional<String> rangeKeyValueOpt) {
        Map<String, AttributeValue> keys = ItemBuilder.builder(hashKeyAttrType, hashKeyValue)
                .rangeKeyStringOpt(rangeKeyValueOpt)
                .build();
        Map<String, AttributeValue> originalKeys = new HashMap<>(keys);
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName).withKey(keys);
        GetItemResult getItemResult = amazonDynamoDb.getItem(getItemRequest);
        assertEquals(tableName, getItemRequest.getTableName()); // assert no side effects
        assertThat(getItemRequest.getKey(), is(originalKeys)); // assert no side effects
        return getItemResult.getItem();
    }

    /**
     * Retrieves the items with the provided PKs (as HKs or HK-RK pairs).
     */
    public static Set<Map<String, AttributeValue>> batchGetItem(ScalarAttributeType hashKeyAttrType,
                                                                AmazonDynamoDB amazonDynamoDb,
                                                                String tableName,
                                                                // TODO?: pass these both as a single list
                                                                List<String> hashKeyValues,
                                                                List<Optional<String>> rangeKeyValueOpts) {
        Preconditions.checkArgument(hashKeyValues.size() == rangeKeyValueOpts.size());
        List<Map<String, AttributeValue>> keys = IntStream.range(0, hashKeyValues.size())
                .mapToObj(i -> ItemBuilder.builder(hashKeyAttrType, hashKeyValues.get(i))
                        .rangeKeyStringOpt(rangeKeyValueOpts.get(i))
                        .build())
                .collect(Collectors.toList());

        Set<Map<String, AttributeValue>> originalKeys = new HashSet<>(keys);
        final KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        keysAndAttributes.setKeys(keys);
        Map<String, KeysAndAttributes> requestItems = ImmutableMap.of(tableName, keysAndAttributes);
        Map<String, KeysAndAttributes> unprocessedKeys = ImmutableMap.of();
        final Set<Map<String, AttributeValue>> resultItems = new HashSet<>();
        do {
            final BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().withRequestItems(requestItems);
            final BatchGetItemResult batchGetItemResult = amazonDynamoDb.batchGetItem(batchGetItemRequest);

            final Set<String> tablesInResult = batchGetItemResult.getResponses().keySet();
            assertEquals(1, tablesInResult.size());
            assertEquals(tableName, Iterables.getOnlyElement(tablesInResult));
            resultItems.addAll(batchGetItemResult.getResponses().get(tableName));
            requestItems = batchGetItemResult.getUnprocessedKeys();
        } while (!unprocessedKeys.isEmpty());
        assertEquals(originalKeys, resultItems.stream()
                .map(TestSupport::stripItemToPk)
                .collect(Collectors.toSet()));
        return resultItems;
    }

    /**
     * Strip {@code item} down to its PK.
     */
    private static Map<String, AttributeValue> stripItemToPk(Map<String, AttributeValue> item) {
        return item.entrySet().stream()
                .filter(p -> p.getKey().equals(HASH_KEY_FIELD) || p.getKey().equals(RANGE_KEY_FIELD))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /*
     * Poll interval and timeout for DDL operations
     */
    public static int getPollInterval() {
        return (IS_LOCAL_DYNAMO ? 0 : 5);
    }

    /*
     * AttributeValue helper methods.
     */

    /**
     * Creates an AttributeValue.
     */
    public static AttributeValue createAttributeValue(ScalarAttributeType keyAttrType, String value) {
        AttributeValue attr = new AttributeValue();
        switch (keyAttrType) {
            case S:
                return attr.withS(value);
            case N:
                return attr.withN(value);
            case B:
                return attr.withB(UTF_8.encode(value));
            default:
                throw new IllegalArgumentException("unsupported type " + keyAttrType + " encountered");
        }
    }

    public static AttributeValue createStringAttribute(String value) {
        return createAttributeValue(S, value);
    }

    /**
     * Converts an AttributeValue into a String.
     */
    public static String attributeValueToString(ScalarAttributeType hashKeyAttrType, AttributeValue attr) {
        switch (hashKeyAttrType) {
            case S:
                return attr.getS();
            case N:
                return attr.getN();
            case B:
                String decodedString = UTF_8.decode(attr.getB()).toString();
                attr.getB().rewind(); // rewind so future readers don't get an empty buffer
                return decodedString;
            default:
                throw new IllegalArgumentException("unsupported type " + hashKeyAttrType + " encountered");
        }
    }
}
