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

    /**
     * Extracts literals referenced in primary and filter expressions and turns them into references to
     * expression names and values.
     */
    void convertFieldNameLiteralsToExpressionNames(RequestWrapper request) {
        Collection<FieldMapping> fieldMappings =
            tableMapping.getAllVirtualToPhysicalFieldMappingsDeduped().values();
        request.setPrimaryExpression(
            convertFieldNameLiteralsToExpressionNamesInternal(fieldMappings, request.getPrimaryExpression(), request));
        request.setFilterExpression(
            convertFieldNameLiteralsToExpressionNamesInternal(fieldMappings, request.getFilterExpression(), request));
    }

    /**
     * For each virtual-physical field mapping, maps field names and applies field value prefixing for tenant isolation.
     */
    void apply(RequestWrapper request) {
        convertFieldNameLiteralsToExpressionNames(request);
        tableMapping.getAllVirtualToPhysicalFieldMappingsDeduped().values().forEach(
            fieldMapping -> applyKeyConditionToField(request, fieldMapping));
    }

    /**
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the mapping, and sets the physical name of the field to that of the target field.
     */
    void applyKeyConditionToField(RequestWrapper request, FieldMapping fieldMapping) {
        applyKeyConditionToField(
            request,
            fieldMapping,
            request.getPrimaryExpression(),
            request.getFilterExpression());
    }

    /**
     * For a given virtual-physical field mapping, maps field names and applies field value prefixing for
     * tenant isolation.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    @VisibleForTesting
    void applyKeyConditionToField(RequestWrapper request,
        FieldMapping fieldMapping,
        String primaryExpression, // "#field1 = :value"
        String filterExpression) {
        if (primaryExpression != null) {
            String virtualAttrName = fieldMapping.getSource().getName(); // "virtualhk"
            Map<String, String> expressionAttrNames = request.getExpressionAttributeNames(); // "#field1" -> "virtualhk"
            Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(virtualAttrName)).map(Entry::getKey).findAny()
                : Optional.empty(); // Optional[#field1]
            if (keyFieldName.isPresent() && !keyFieldName.get().equals(NAME_PLACEHOLDER)) {
                Optional<String> virtualValuePlaceholderOpt =
                    findVirtualValuePlaceholder(primaryExpression, filterExpression, keyFieldName.get()); // ":value"
                if (virtualValuePlaceholderOpt.isPresent()) {
                    String virtualValuePlaceholder = virtualValuePlaceholderOpt.get();
                    AttributeValue virtualAttr =
                            request.getExpressionAttributeValues().get(virtualValuePlaceholder); // {S: hkvalue,}
                    AttributeValue physicalAttr =
                        fieldMapping.isContextAware()
                                ? fieldMapper.apply(fieldMapping, virtualAttr) // {S: ctx.virtualTable.hkvalue,}
                                : virtualAttr;
                    request.putExpressionAttributeValue(virtualValuePlaceholder, physicalAttr);
                }
                request.putExpressionAttributeName(keyFieldName.get(), fieldMapping.getTarget().getName());
            }
        }
    }

    /**
     * Extracts literals referenced in expressions and turns them into references to expression names and values.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    private String convertFieldNameLiteralsToExpressionNamesInternal(
        Collection<FieldMapping> fieldMappings, // source = "field", target="field"
        String conditionExpression, // "field = :value and field2 = :value2 and field = :value3"
        RequestWrapper request) {
        String newConditionExpression = conditionExpression;
        if (conditionExpression != null) {
            AtomicInteger counter = new AtomicInteger(1);
            for (FieldMapping fieldMapping : fieldMappings) {
                String virtualFieldName = fieldMapping.getSource().getName(); // "field"
                String toFind = " " + virtualFieldName + " ="; // " field ="
                int start = (" " + newConditionExpression).indexOf(toFind); // 0 - TODO add support for non-EQ operators
                while (start >= 0) {
                    String fieldLiteral =
                            newConditionExpression.substring(start, start + virtualFieldName.length()); // "field"
                    String fieldPlaceholder =
                            getNextFieldPlaceholder(request.getExpressionAttributeNames(), counter); // "#field1"
                    newConditionExpression = conditionExpression
                        .replaceAll(fieldLiteral + " ", fieldPlaceholder + " ");
                    // "#field1 = :value and field2 = :value2 and #field1 = :value3"
                    request.putExpressionAttributeName(fieldPlaceholder, fieldLiteral);
                    start = (" " + newConditionExpression).indexOf(toFind); // -1
                }
            }
        }
        return newConditionExpression;
    }

    /*
     * Generates a field name placeholder starting with '#field' and suffixed with an incrementing number skipping
     * any reference that already exists in the expressionAttributeNames map.
     */
    @VisibleForTesting
    static String getNextFieldPlaceholder(Map<String, String> expressionAttributeNames, AtomicInteger counter) {
        String fieldPlaceholderCandidate = "#field" + counter.get();
        while (expressionAttributeNames != null && expressionAttributeNames.containsKey(fieldPlaceholderCandidate)) {
            fieldPlaceholderCandidate = "#field" + counter.incrementAndGet();
        }
        return fieldPlaceholderCandidate;
    }

    /**
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

    /**
     * Finds the value in the right-hand side operand of an expression where the left-hand operator is a given field.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
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
        int end = conditionExpression.indexOf(" ", start + toFind.length()); // -1
        return Optional.of(conditionExpression.substring(start + toFind.length(),
            end == -1 ? conditionExpression.length() : end)); // ":value" // TODO add support for non-EQ operators
    }

}