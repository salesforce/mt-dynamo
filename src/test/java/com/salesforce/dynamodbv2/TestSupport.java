package com.salesforce.dynamodbv2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author msgroi
 */
public class TestSupport {

    static final boolean IS_LOCAL_DYNAMO = true; // TODO msgroi should test running against hosted dynamo
    static final int TIMEOUT_SECONDS = 60;
    static final String HASH_KEY_FIELD = "hashKeyField";
    static final String HASH_KEY_VALUE = "1";
    static final String RANGE_KEY_FIELD = "rangeKeyField";
    static final String RANGE_KEY_VALUE = "rangeKeyValue";
    static final String SOME_FIELD = "someField";
    static final String SOME_FIELD_VALUE = "someValue";
    static final String INDEX_FIELD = "indexField";
    static final String INDEX_FIELD_VALUE = "indexFieldValue";

    /*
     * get helper methods
     */

    public static Map<String, AttributeValue> getItem(ScalarAttributeType hashKeyAttrType, AmazonDynamoDB amazonDynamoDB, String tableName) {
        return getItem(hashKeyAttrType, amazonDynamoDB, tableName, HASH_KEY_VALUE);
    }

    static Map<String, AttributeValue> getHkRkItem(ScalarAttributeType hashKeyAttrType, AmazonDynamoDB amazonDynamoDB, String tableName) {
        return getItem(hashKeyAttrType, amazonDynamoDB, tableName, HASH_KEY_VALUE, Optional.of(RANGE_KEY_VALUE));
    }

    public static Map<String, AttributeValue> getItem(ScalarAttributeType hashKeyAttrType, AmazonDynamoDB amazonDynamoDB, String tableName, String hashKeyValue) {
        return getItem(hashKeyAttrType, amazonDynamoDB, tableName, hashKeyValue, Optional.empty());
    }

    static Map<String, AttributeValue> getItem(ScalarAttributeType hashKeyAttrType,
        AmazonDynamoDB amazonDynamoDB,
        String tableName,
        String hashKeyValue,
        Optional<String> rangeKeyValue) {
        Map<String, AttributeValue> keys = rangeKeyValue
            .map(s -> new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType,
                hashKeyValue),
                RANGE_KEY_FIELD, createStringAttribute(s))))
            .orElseGet(() -> new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD, createHkAttribute(
                hashKeyAttrType, hashKeyValue))));
        Map<String, AttributeValue> originalKeys = new HashMap<>(keys);
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName).withKey(keys);
        GetItemResult getItemResult = amazonDynamoDB.getItem(getItemRequest);
        assertEquals(tableName, getItemRequest.getTableName()); // assert no side effects
        assertThat(getItemRequest.getKey(), is(originalKeys)); // assert no side effects
        return getItemResult.getItem();
    }

    /*
     * Poll interval and timeout for DDL operations
     */
    static int getPollInterval() {
        return (IS_LOCAL_DYNAMO ? 0 : 5);
    }

    /*
     * AttributeValue helper methods
     */

    public static AttributeValue createHkAttribute(ScalarAttributeType hashKeyAttrType, String value) {
        AttributeValue attr = new AttributeValue();
        switch (hashKeyAttrType) {
            case S:
                attr.setS(value);
                return attr;
            case N:
                attr.setN(value);
                return attr;
            case B:
                attr.setB(UTF_8.encode(value));
                return attr;
            default:
                throw new IllegalArgumentException("unsupported type " + hashKeyAttrType + " encountered");
        }
    }

    static String attributeValueToString(ScalarAttributeType hashKeyAttrType, AttributeValue attr) {
        switch (hashKeyAttrType) {
            case B:
                String decodedString = UTF_8.decode(attr.getB()).toString();
                attr.getB().rewind(); // rewind so future readers don't get an empty buffer
                return decodedString;
            case N:
                return attr.getN();
            case S:
                return attr.getS();
            default:
                throw new IllegalArgumentException("unsupported type " + hashKeyAttrType + " encountered");
        }
    }

    static AttributeValue createStringAttribute(String value) {
        return new AttributeValue().withS(value);
    }

    /*
     * item building helper methods
     */

    static Map<String, AttributeValue> buildItemWithSomeFieldValue(ScalarAttributeType hashKeyAttrType, String value) {
        Map<String, AttributeValue> item = defaultItem(hashKeyAttrType);
        item.put(SOME_FIELD, createStringAttribute(value));
        return item;
    }

    static Map<String, AttributeValue> buildItemWithValues(ScalarAttributeType hashKeyAttrType,
        String hashKeyValue,
        Optional<String> rangeKeyValue,
        String someFieldValue) {
        return buildItemWithValues(hashKeyAttrType, hashKeyValue, rangeKeyValue, someFieldValue, Optional.empty());
    }

    static Map<String, AttributeValue> buildItemWithValues(ScalarAttributeType hashKeyAttrType,
        String hashKeyValue,
        Optional<String> rangeKeyValueOpt,
        String someFieldValue,
        Optional<String> indexFieldValueOpt) {
        Map<String, AttributeValue> item = defaultItem(hashKeyAttrType);
        item.put(HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType, hashKeyValue));
        rangeKeyValueOpt.ifPresent(rangeKeyValue -> item.put(RANGE_KEY_FIELD, createStringAttribute(rangeKeyValue)));
        item.put(SOME_FIELD, createStringAttribute(someFieldValue));
        indexFieldValueOpt.ifPresent(indexFieldValue -> item.put(INDEX_FIELD, createStringAttribute(indexFieldValue)));
        return item;
    }

    static Map<String, AttributeValue> buildHkRkItemWithSomeFieldValue(ScalarAttributeType hashKeyAttrType, String value) {
        Map<String, AttributeValue> item = defaultHkRkItem(hashKeyAttrType);
        item.put(SOME_FIELD, createStringAttribute(value));
        return item;
    }

    static Map<String, AttributeValue> buildKey(ScalarAttributeType hashKeyAttrType) {
        return new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType,
            HASH_KEY_VALUE)));
    }

    static Map<String, AttributeValue> buildHkRkKey(ScalarAttributeType hashKeyAttrType) {
        return new HashMap<>(new HashMap<>(ImmutableMap.of(
            HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType, HASH_KEY_VALUE),
            RANGE_KEY_FIELD, createStringAttribute(RANGE_KEY_VALUE))));
    }

    /*
     * private
     */

    private static Map<String, AttributeValue> defaultItem(ScalarAttributeType hashKeyAttrType) {
        return new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType, HASH_KEY_VALUE),
            SOME_FIELD, createStringAttribute(SOME_FIELD_VALUE)));
    }

    private static Map<String, AttributeValue> defaultHkRkItem(ScalarAttributeType hashKeyAttrType) {
        return new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD, createHkAttribute(hashKeyAttrType, HASH_KEY_VALUE),
            RANGE_KEY_FIELD, createStringAttribute(RANGE_KEY_VALUE),
            SOME_FIELD, createStringAttribute(SOME_FIELD_VALUE)));
    }

}