package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Helper methods for parsing and mapping expressions.
 *
 * @author msgroi
 */
class ExpressionMappingSupport {

    static final String NAME_PLACEHOLDER = "#___name___";

    static void convertFieldNameLiteralsToExpressionNames(Collection<FieldMapping> fieldMappings,
        RequestWrapper request) {
        request.setPrimaryExpression(
            convertFieldNameLiteralsToExpressionNames(fieldMappings, request.getPrimaryExpression(), request));
        request.setFilterExpression(
            convertFieldNameLiteralsToExpressionNames(fieldMappings, request.getFilterExpression(), request));
    }

    private static String convertFieldNameLiteralsToExpressionNames(Collection<FieldMapping> fieldMappings,
        String conditionExpression,
        RequestWrapper request) {
        String newConditionExpression = conditionExpression;
        if (conditionExpression != null) {
            AtomicInteger counter = new AtomicInteger(1);
            for (FieldMapping fieldMapping : fieldMappings) {
                String virtualFieldName = fieldMapping.getSource().getName();
                String toFind = " " + virtualFieldName + " =";
                int start = (" " + newConditionExpression).indexOf(toFind); // TODO add support for non-EQ operators
                while (start >= 0) {
                    String fieldLiteral = newConditionExpression.substring(start, start + virtualFieldName.length());
                    String fieldPlaceholder = getNextFieldPlaceholder(request.getExpressionAttributeNames(), counter);
                    newConditionExpression = conditionExpression
                        .replaceAll(fieldLiteral + " ", fieldPlaceholder + " ");
                    request.putExpressionAttributeName(fieldPlaceholder, fieldLiteral);
                    start = (" " + newConditionExpression).indexOf(toFind);
                }
            }
        }
        return newConditionExpression;
    }

    /*
     * This method takes a mapping of virtual to physical fields, where it is possible that a single given virtual
     * field may map to more than one physical field, and returns a mapping where each virtual field maps to exactly
     * one physical field.  In cases where there is more than one physical field for a given virtual field, it
     * arbitrarily chooses the first mapping.
     *
     * This method is called for any query or scan request that does not specify an index.
     *
     * It is an effective no-op, meaning, there are no duplicates to remove, except when a scan is performed against
     * a table that maps a given virtual field to multiple physical fields.  In that case, it doesn't matter which
     * field we use in the query, the results should be the same, so we choose one of the physical fields arbitrarily.
     */
    static Map<String, FieldMapping> dedupeFieldMappings(Map<String, List<FieldMapping>> fieldMappings) {
        return fieldMappings.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey,
            fieldMappingEntry -> fieldMappingEntry.getValue().get(0)
        ));
    }

    /**
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the mapping, and sets the physical name of the field to that of the target field.
     */
    static void applyKeyConditionToField(FieldMapper fieldMapper, RequestWrapper request, FieldMapping fieldMapping) {
        applyKeyConditionToField(fieldMapper,
            request,
            fieldMapping,
            request.getPrimaryExpression(),
            request.getFilterExpression());
    }

    private static void applyKeyConditionToField(FieldMapper fieldMapper, RequestWrapper request,
        FieldMapping fieldMapping,
        String primaryExpression,
        String filterExpression) {
        if (primaryExpression != null) {
            String virtualAttrName = fieldMapping.getSource().getName();
            Map<String, String> expressionAttrNames = request.getExpressionAttributeNames();
            Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(virtualAttrName)).map(Entry::getKey).findAny()
                : Optional.empty();
            if (keyFieldName.isPresent() && !keyFieldName.get().equals(NAME_PLACEHOLDER)) {
                String virtualValuePlaceholder = findVirtualValuePlaceholder(primaryExpression, filterExpression,
                    keyFieldName.get());
                AttributeValue virtualAttr = request.getExpressionAttributeValues().get(virtualValuePlaceholder);
                AttributeValue physicalAttr =
                    fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, virtualAttr) : virtualAttr;
                request.putExpressionAttributeValue(virtualValuePlaceholder, physicalAttr);
                request.putExpressionAttributeName(keyFieldName.get(), fieldMapping.getTarget().getName());
            }
        }
    }

    private static String getNextFieldPlaceholder(Map<String, String> expressionAttributeNames, AtomicInteger counter) {
        String fieldPlaceholderCandidate = "#field" + counter.get();
        while (expressionAttributeNames != null && expressionAttributeNames.containsKey(fieldPlaceholderCandidate)) {
            fieldPlaceholderCandidate = "#field" + counter.incrementAndGet();
        }
        return fieldPlaceholderCandidate;
    }

    /*
     * Finds the value in the right-hand side operand where the left-hand operator is a given field, first in the
     * primary expression, then in the filterExpression.
     */
    private static String findVirtualValuePlaceholder(String primaryExpression,
        String filterExpression,
        String keyFieldName) {
        return findVirtualValuePlaceholder(primaryExpression, keyFieldName)
            .orElseGet((Supplier<String>) () -> findVirtualValuePlaceholder(filterExpression, keyFieldName)
                .orElseThrow((Supplier<IllegalArgumentException>) () ->
                    new IllegalArgumentException("field " + keyFieldName + " not found in either conditionExpression="
                        + primaryExpression + ", or filterExpression=" + filterExpression)));
    }

    /*
     * Finds the value in the right-hand side operand of an expression where the left-hand operator is a given field.
     */
    private static Optional<String> findVirtualValuePlaceholder(String conditionExpression, String keyFieldName) {
        String toFind = keyFieldName + " = ";
        int start = conditionExpression.indexOf(toFind);
        if (start == -1) {
            return Optional.empty();
        }
        int end = conditionExpression.indexOf(" ", start + toFind.length());
        return Optional.of(conditionExpression.substring(start + toFind.length(),
            end == -1 ? conditionExpression.length() : end)); // TODO add support for non-EQ operators
    }

    @VisibleForTesting
    public interface RequestWrapper {

        String getIndexName();

        Map<String, String> getExpressionAttributeNames();

        void putExpressionAttributeName(String key, String value);

        Map<String, AttributeValue> getExpressionAttributeValues();

        void putExpressionAttributeValue(String key, AttributeValue value);

        String getPrimaryExpression();

        void setPrimaryExpression(String expression);

        String getFilterExpression();

        void setFilterExpression(String s);

        void setIndexName(String indexName);

        Map<String, Condition> getLegacyExpression();

        void clearLegacyExpression();

        Map<String, AttributeValue> getExclusiveStartKey();

        void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey);
    }

}