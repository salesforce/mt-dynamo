package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Applies mapping and prefixing to condition query and conditional update expressions.
 *
 * @author msgroi
 */
class ConditionMapper {

    static final String NAME_PLACEHOLDER = "#___name___";

    private final TableMapping tableMapping;
    private final FieldMapper fieldMapper;

    ConditionMapper(TableMapping tableMapping, FieldMapper fieldMapper) {
        this.tableMapping = tableMapping;
        this.fieldMapper = fieldMapper;
    }

    void convertFieldNameLiteralsToExpressionNames(RequestWrapper request) {
        Collection<FieldMapping> fieldMappings =
            tableMapping.getAllVirtualToPhysicalFieldMappingsDeduped().values();
        request.setPrimaryExpression(
            convertFieldNameLiteralsToExpressionNamesInternal(fieldMappings, request.getPrimaryExpression(), request));
        request.setFilterExpression(
            convertFieldNameLiteralsToExpressionNamesInternal(fieldMappings, request.getFilterExpression(), request));
    }

    void apply(RequestWrapper request) { // TODO msgroi unit test
        convertFieldNameLiteralsToExpressionNames(request);
        tableMapping.getAllVirtualToPhysicalFieldMappingsDeduped().values().forEach(
            fieldMapping -> applyKeyConditionToField(request, fieldMapping));
    }

    /**
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the mapping, and sets the physical name of the field to that of the target field.
     */
    void applyKeyConditionToField(RequestWrapper request, FieldMapping fieldMapping) { // TODO msgroi unit test
        applyKeyConditionToField(
            request,
            fieldMapping,
            request.getPrimaryExpression(),
            request.getFilterExpression());
    }

    @VisibleForTesting
    void applyKeyConditionToField(RequestWrapper request, // TODO msgroi unit test
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
                Optional<String> virtualValuePlaceholderOpt =
                    findVirtualValuePlaceholder(primaryExpression, filterExpression, keyFieldName.get());
                if (virtualValuePlaceholderOpt.isPresent()) {
                    String virtualValuePlaceholder = virtualValuePlaceholderOpt.get();
                    AttributeValue virtualAttr = request.getExpressionAttributeValues().get(virtualValuePlaceholder);
                    AttributeValue physicalAttr =
                        fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, virtualAttr) : virtualAttr;
                    request.putExpressionAttributeValue(virtualValuePlaceholder, physicalAttr);
                }
                request.putExpressionAttributeName(keyFieldName.get(), fieldMapping.getTarget().getName());
            }
        }
    }

    private String convertFieldNameLiteralsToExpressionNamesInternal(Collection<FieldMapping> fieldMappings,
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

    @VisibleForTesting
    static String getNextFieldPlaceholder(Map<String, String> expressionAttributeNames, AtomicInteger counter) {
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
    @VisibleForTesting
    static Optional<String> findVirtualValuePlaceholder(String primaryExpression,
        String filterExpression,
        String keyFieldName) {
        return Stream.of(
            findVirtualValuePlaceholder(primaryExpression, keyFieldName),
            findVirtualValuePlaceholder(filterExpression, keyFieldName))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

    /*
     * Finds the value in the right-hand side operand of an expression where the left-hand operator is a given field.
     */
    @VisibleForTesting
    static Optional<String> findVirtualValuePlaceholder(String conditionExpression, String keyFieldName) {
        if (conditionExpression == null) {
            return Optional.empty();
        }
        String toFind = keyFieldName + " = ";
        int start = conditionExpression.indexOf(toFind);
        if (start == -1) {
            return Optional.empty();
        }
        int end = conditionExpression.indexOf(" ", start + toFind.length());
        return Optional.of(conditionExpression.substring(start + toFind.length(),
            end == -1 ? conditionExpression.length() : end)); // TODO add support for non-EQ operators
    }

}