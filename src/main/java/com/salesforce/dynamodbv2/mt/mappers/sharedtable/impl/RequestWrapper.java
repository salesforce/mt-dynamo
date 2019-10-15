package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Wrapper class that allows application code to handle {@code QueryRequest}s, {@code ScanRequest}s,
 * {@code PutItemRequest}s, {@code UpdateItemRequest}s, and {@code DeleteItemRequest}s consistently.
 *
 * @author msgroi
 */
interface RequestWrapper {

    String getExpression();

    void setExpression(String expression);

    Map<String, String> getExpressionAttributeNames();

    void putExpressionAttributeName(String key, String value);

    Map<String, AttributeValue> getExpressionAttributeValues();

    void putExpressionAttributeValue(String key, AttributeValue value);

    abstract class AbstractRequestWrapper implements RequestWrapper {

        private final Supplier<Map<String, String>> attributeNameMapGetter;
        private final Consumer<Map<String, String>> attributeNameMapSetter;
        private final Supplier<Map<String, AttributeValue>> attributeValueMapGetter;
        private final Consumer<Map<String, AttributeValue>> attributeValueMapSetter;
        private final Supplier<String> expressionGetter;
        private final Consumer<String> expressionSetter;

        AbstractRequestWrapper(Supplier<Map<String, String>> attributeNameMapGetter,
                       Consumer<Map<String, String>> attributeNameMapSetter,
                       Supplier<Map<String, AttributeValue>> attributeValueMapGetter,
                       Consumer<Map<String, AttributeValue>> attributeValueMapSetter,
                       @Nullable Supplier<String> expressionGetter,
                       @Nullable Consumer<String> expressionSetter) {
            this.attributeNameMapGetter = attributeNameMapGetter;
            this.attributeNameMapSetter = attributeNameMapSetter;
            this.attributeValueMapGetter = attributeValueMapGetter;
            this.attributeValueMapSetter = attributeValueMapSetter;
            attributeNameMapSetter.accept(getMutableMap(attributeNameMapGetter.get()));
            attributeValueMapSetter.accept(getMutableMap(attributeValueMapGetter.get()));
            this.expressionGetter = expressionGetter;
            this.expressionSetter = expressionSetter;
        }

        @Override
        public String getExpression() {
            if (expressionGetter == null) {
                throw new UnsupportedOperationException("No original expression in request wrapper");
            }
            return expressionGetter.get();
        }

        @Override
        public void setExpression(String expression) {
            if (expressionSetter == null) {
                throw new UnsupportedOperationException("Cannot set expression in request wrapper");
            }
            expressionSetter.accept(expression);
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return attributeNameMapGetter.get();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (attributeNameMapGetter.get() == null) {
                attributeNameMapSetter.accept(new HashMap<>());
            }
            attributeNameMapGetter.get().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            return attributeValueMapGetter.get();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            if (attributeValueMapGetter.get() == null) {
                attributeValueMapSetter.accept(new HashMap<>());
            }
            attributeValueMapGetter.get().put(key, value);
        }

        private static <K, V> Map<K, V> getMutableMap(Map<K, V> potentiallyImmutableMap) {
            return (potentiallyImmutableMap == null) ? null : new HashMap<>(potentiallyImmutableMap);
        }
    }

}