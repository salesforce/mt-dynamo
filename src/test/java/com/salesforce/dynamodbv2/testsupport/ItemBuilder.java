package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Builds a map of attribute names to {@code AttributeValue}s.
 */
public class ItemBuilder {
    public static final String HASH_KEY_FIELD = "hashKeyField";
    public static final String RANGE_KEY_FIELD = "rangeKeyField";
    public static final String SOME_FIELD = "someField";
    public static final String SOME_OTHER_FIELD = "someOtherField";
    public static final String SOME_OTHER_OTHER_FIELD = "someOtherOtherField";
    public static final String INDEX_FIELD = "indexField";
    public static final String GSI_HK_FIELD = "gsiHkField";
    public static final String GSI2_HK_FIELD = "gsi2HkField";
    public static final String GSI2_RK_FIELD = "gsi2RkField";

    private final Map<String, AttributeValue> item;

    /**
     * Creates a new ItemBuilder.
     */
    public static ItemBuilder builder(ScalarAttributeType hashKeyAttrType, String hashKeyValue) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, hashKeyValue));

        return new ItemBuilder(item);
    }

    private ItemBuilder(Map<String, AttributeValue> item) {
        this.item = item;
    }

    public ItemBuilder rangeKey(ScalarAttributeType rangeKeyAttrType, String rangeKeyValue) {
        this.item.put(RANGE_KEY_FIELD, createAttributeValue(rangeKeyAttrType, rangeKeyValue));
        return this;
    }

    /**
     * Add a range key with value rangeKeyValueOpt.get() if present, otherwise do nothing.
     */
    public ItemBuilder rangeKeyStringOpt(Optional<String> rangeKeyValueOpt) {
        return rangeKeyValueOpt.map(rangeKeyValue -> this.rangeKey(S, rangeKeyValue)).orElse(this);
    }

    public ItemBuilder someField(ScalarAttributeType type, String someFieldValue) {
        this.item.put(SOME_FIELD, createAttributeValue(type, someFieldValue));
        return this;
    }

    public ItemBuilder someOtherField(ScalarAttributeType type, String someOtherFieldValue) {
        this.item.put(SOME_OTHER_FIELD, createAttributeValue(type, someOtherFieldValue));
        return this;
    }

    public ItemBuilder someOtherOtherField(ScalarAttributeType type, String someOtherOtherFieldValue) {
        this.item.put(SOME_OTHER_OTHER_FIELD, createAttributeValue(type, someOtherOtherFieldValue));
        return this;
    }

    public ItemBuilder indexField(ScalarAttributeType type, String indexFieldValue) {
        this.item.put(INDEX_FIELD, createAttributeValue(type, indexFieldValue));
        return this;
    }

    public ItemBuilder gsiHkField(ScalarAttributeType type, String value) {
        this.item.put(GSI_HK_FIELD, createAttributeValue(type, value));
        return this;
    }

    public ItemBuilder gsi2HkField(ScalarAttributeType type, String value) {
        this.item.put(GSI2_HK_FIELD, createAttributeValue(type, value));
        return this;
    }

    public ItemBuilder gsi2RkField(ScalarAttributeType type, String value) {
        this.item.put(GSI2_RK_FIELD, createAttributeValue(type, value));
        return this;
    }

    public ItemBuilder putAll(Map<String, AttributeValue> fieldValues) {
        this.item.putAll(fieldValues);
        return this;
    }

    public ImmutableMap<String, AttributeValue> build() {
        return ImmutableMap.copyOf(this.item);
    }

    @Override
    public String toString() {
        return this.item.toString();
    }
}
