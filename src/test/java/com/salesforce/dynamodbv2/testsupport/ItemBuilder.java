package com.salesforce.dynamodbv2.testsupport;

import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.Map;

// TODO: change input parameter from AttributeValue to ScalarAttributeType (keyAttrType), String (value)
/**
 * Builds a map of attribute names to {@code AttributeValue} objects.
 */
public class ItemBuilder {
    final Map<String, AttributeValue> item;

    /**
     * Creates a new builder.
     */
    public static ItemBuilder builder(AttributeValue hashKeyValue) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(HASH_KEY_FIELD, hashKeyValue);

        return new ItemBuilder(item);
    }

    private ItemBuilder(Map<String, AttributeValue> item) {
        this.item = item;
    }

    public ItemBuilder rangeKey(AttributeValue rangeKeyValue) {
        this.item.put(RANGE_KEY_FIELD, rangeKeyValue);
        return this;
    }

    public ItemBuilder someField(AttributeValue someFieldValue) {
        this.item.put(SOME_FIELD, someFieldValue);
        return this;
    }

    public ItemBuilder indexField(AttributeValue indexFieldValue) {
        this.item.put(INDEX_FIELD, indexFieldValue);
        return this;
    }

    public Map<String, AttributeValue> build() {
        return this.item;
    }

    @Override
    public String toString() {
        // return this.item.toString();
        return Joiner.on(",").withKeyValueSeparator("=").join(this.item);
    }
}
