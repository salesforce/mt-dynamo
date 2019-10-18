package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextFieldPlaceholder;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextPlaceholder;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextValuePlaceholder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RequestWrapper.AbstractRequestWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * {@link ConditionMapper} implementation for shared tables using random partitioning.
 *
 * @author msgroi
 */
class RandomPartitioningConditionMapper implements ConditionMapper {

    static final String NAME_PLACEHOLDER = "#___name___";
    private static final String COMPARISON_OP_PATTERN = "(=|<>|<|<=|>|>=)";
    private static final Pattern ATTR_NAME_PLACEHOLDER_PATTERN = Pattern.compile("#[a-zA-Z0-9]+");

    private final RandomPartitioningTableMapping tableMapping;
    private final FieldMapper fieldMapper;

    RandomPartitioningConditionMapper(RandomPartitioningTableMapping tableMapping, FieldMapper fieldMapper) {
        this.tableMapping = tableMapping;
        this.fieldMapper = fieldMapper;
    }

    @Override
    public void applyForUpdate(UpdateItemRequest updateItemRequest) {
        RequestWrapper updateExprWrapper = new UpdateExpressionRequestWrapper(updateItemRequest);
        Set<String> virtualFieldNames = tableMapping.getAllMappingsPerField().keySet();

        convertFieldNameLiteralsToExpressionNames(updateExprWrapper, virtualFieldNames);
        validateNotUpdatingTablePrimaryKeyFields(updateExprWrapper);

        if (updateItemRequest.getConditionExpression() != null) {
            UpdateConditionExpressionRequestWrapper conditionExprWrapper =
                new UpdateConditionExpressionRequestWrapper(updateItemRequest);
            convertFieldNameLiteralsToExpressionNames(conditionExprWrapper, virtualFieldNames);
            makePlaceholdersDistinct(updateItemRequest.getUpdateExpression(), conditionExprWrapper);
            applyToFilterExpression(conditionExprWrapper);
        }

        for (Entry<String, List<FieldMapping>> entry : tableMapping.getAllMappingsPerField().entrySet()) {
            mapFieldInUpdateExpression(updateExprWrapper, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void applyToKeyCondition(RequestWrapper request, RequestIndex requestIndex, String filterExpression) {
        Collection<FieldMapping> fieldMappings = requestIndex.getVirtualSecondaryIndex().isPresent()
            ? tableMapping.getIndexPrimaryKeyFieldMappings(requestIndex.getVirtualSecondaryIndex().get())
            : tableMapping.getTablePrimaryKeyFieldMappings();

        // if the hash key value placeholder is also in the filter expression, then we need to use two different
        // value placeholders, since the hash key value will become different but the other not
        if (filterExpression != null) {
            makePlaceholdersDistinct(filterExpression, request);
        }

        mapFieldsInConditionExpression(request, fieldMappings);
    }

    @Override
    public void applyToFilterExpression(RequestWrapper request) {
        // for conditional / filter expressions, for each indexed field, we only need to use one of its field mappings,
        // since any of the possible physical field comparison expressions are true IFF virtual field comparison is true
        List<FieldMapping> fieldMappings = new ArrayList<>(tableMapping.getAllMappingsPerField().size());
        tableMapping.getAllMappingsPerField().forEach((virtualField, mappings) -> fieldMappings.add(mappings.get(0)));

        mapFieldsInConditionExpression(request, fieldMappings);
    }

    /**
     * Used by applyToKeyCondition() and applyToFilterExpression() to apply the given field mappings to a
     * condition/filter expression.
     */
    private void mapFieldsInConditionExpression(RequestWrapper request, Collection<FieldMapping> fieldMappings) {
        Set<String> virtualFieldNames = fieldMappings.stream()
            .map(m -> m.getSource().getName())
            .collect(Collectors.toSet());
        convertFieldNameLiteralsToExpressionNames(request, virtualFieldNames);

        fieldMappings.forEach(fieldMapping -> mapFieldInConditionExpression(request, fieldMapping));
    }

    /**
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the mapping, and sets the physical name of the field to that of the target field.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    @VisibleForTesting
    void mapFieldInConditionExpression(RequestWrapper request,
                                       FieldMapping fieldMapping) {
        String expression = request.getExpression(); // "#field1 = :value"
        if (expression != null) {
            String virtualField = fieldMapping.getSource().getName();
            Set<String> fieldPlaceholders = getPlaceholdersForVirtualField(virtualField, request); // {#field1}

            for (String fieldPlaceholder : fieldPlaceholders) {
                // TODO support one virtual field placeholder having multiple comparison clauses in an expression
                Optional<String> valuePlaceholderOpt =
                    findVirtualValuePlaceholder(expression, fieldPlaceholder); // "Optional[:value]"
                if (valuePlaceholderOpt.isPresent()) {
                    String valuePlaceholder = valuePlaceholderOpt.get();
                    AttributeValue virtualValue = request.getExpressionAttributeValues()
                        .get(valuePlaceholder); // {S: hkValue,}
                    AttributeValue physicalValue = fieldMapping.isContextAware()
                        ? fieldMapper.apply(fieldMapping, virtualValue) // {S: ctx.virtualTable.hkValue,}
                        : virtualValue;
                    request.putExpressionAttributeValue(valuePlaceholder, physicalValue);
                }
                request.putExpressionAttributeName(fieldPlaceholder, fieldMapping.getTarget().getName());
            }
            request.setExpression(expression);
        }
    }

    private void mapFieldInUpdateExpression(RequestWrapper request,
                                            String virtualField, // "virtualHk"
                                            List<FieldMapping> fieldMappings) {
        String expression = request.getExpression(); // "#field1 = :value"
        if (expression != null) {
            Set<String> virtualFieldPlaceholders = getPlaceholdersForVirtualField(virtualField, request); // {#field1};
            for (String virtualFieldPlaceholder : virtualFieldPlaceholders) {
                // each placeholder should appear at most once in update expression, assuming we support only SET
                Optional<String> virtualValuePlaceholderOpt =
                    findVirtualValuePlaceholder(expression, virtualFieldPlaceholder); // "Optional[:value]"
                if (virtualValuePlaceholderOpt.isPresent()) {
                    String virtualValuePlaceholder = virtualValuePlaceholderOpt.get();
                    AttributeValue virtualValue =
                        request.getExpressionAttributeValues().get(virtualValuePlaceholder); // {S: hkValue,}

                    List<String> equalsClauses = new ArrayList<>(fieldMappings.size());
                    for (FieldMapping fieldMapping : fieldMappings) {
                        String physicalField = fieldMapping.getTarget().getName(); // physicalHk
                        String physicalFieldPlaceholder = equalsClauses.isEmpty()
                            ? virtualFieldPlaceholder
                            : getNextFieldPlaceholder(request);
                        request.putExpressionAttributeName(physicalFieldPlaceholder, physicalField);

                        AttributeValue physicalValue = fieldMapping.isContextAware()
                            ? fieldMapper.apply(fieldMapping, virtualValue) // {S: ctx.virtualTable.hkValue,}
                            : virtualValue;
                        String physicalValuePlaceholder = equalsClauses.isEmpty()
                            ? virtualValuePlaceholder
                            : getNextValuePlaceholder(request);
                        request.putExpressionAttributeValue(physicalValuePlaceholder, physicalValue);

                        equalsClauses.add(physicalFieldPlaceholder + " = " + physicalValuePlaceholder);
                    }

                    // possibly fan out to assigning multiple fields for SET
                    String physicalClause = String.join(", ", equalsClauses);
                    expression = expression.replaceAll(
                        virtualFieldPlaceholder + " *= *" + virtualValuePlaceholder + "\\b", physicalClause);
                }
            }
            request.setExpression(expression);
        }
    }

    private void validateNotUpdatingTablePrimaryKeyFields(RequestWrapper request) {
        if (request.getExpression() != null) {
            PrimaryKey primaryKey = tableMapping.getVirtualTable().getPrimaryKey();
            Set<String> tablePrimaryKeyFields = new HashSet<>();
            tablePrimaryKeyFields.add(primaryKey.getHashKey());
            primaryKey.getRangeKey().ifPresent(tablePrimaryKeyFields::add);

            Matcher m = ATTR_NAME_PLACEHOLDER_PATTERN.matcher(request.getExpression());
            while (m.find()) {
                String placeholder = m.group();
                String field = request.getExpressionAttributeNames().get(placeholder);
                if (field != null && tablePrimaryKeyFields.contains(field)) {
                    // not the best error message but this is what local dynamo does?
                    throw new AmazonServiceException("This attribute is part of the key");
                }
            }
        }
    }

    private Set<String> getPlaceholdersForVirtualField(String virtualField, RequestWrapper request) {
        return Optional.ofNullable(request.getExpressionAttributeNames())
            .orElse(Collections.emptyMap())
            .entrySet().stream()
            .filter(entry -> entry.getValue().equals(virtualField) && !entry.getKey().equals(NAME_PLACEHOLDER))
            .map(Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Extracts literals referenced in an expression and turns them into references to expression names and values.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    @VisibleForTesting
    static void convertFieldNameLiteralsToExpressionNames(
        RequestWrapper request, // expression: "literal1 = :value AND literal1 = :value2"
        Collection<String> virtualFieldNames // ["literal1", "literal2"]
    ) {
        String expression = request.getExpression();
        if (expression != null) {
            for (String virtualFieldName : virtualFieldNames) {
                Pattern p = Pattern.compile("(^| )((?<comparison>" + virtualFieldName + ") *" + COMPARISON_OP_PATTERN
                    + "|attribute_exists\\((?<exists>" + virtualFieldName + ")\\)"
                    + "|attribute_not_exists\\((?<notexists>" + virtualFieldName + ")\\))");
                Matcher m = p.matcher(request.getExpression());
                List<Integer> starts = new ArrayList<>();
                while (m.find()) {
                    if (m.start("comparison") >= 0) {
                        starts.add(m.start("comparison"));
                    }
                    if (m.start("exists") >= 0) {
                        starts.add(m.start("exists"));
                    }
                    if (m.start("notexists") >= 0) {
                        starts.add(m.start("notexists"));
                    }
                }
                if (!starts.isEmpty()) {    // [0, 22]
                    String placeholder = getNextFieldPlaceholder(request);
                    request.putExpressionAttributeName(placeholder, virtualFieldName); // #field1 -> literal1

                    StringBuilder newExpression = new StringBuilder();
                    Iterator<Integer> startsIterator = starts.iterator();
                    int previousEnd = 0;
                    do {
                        int start = startsIterator.next();
                        newExpression.append(expression, previousEnd, start).append(placeholder);
                        previousEnd = start + virtualFieldName.length();
                    } while (startsIterator.hasNext());
                    newExpression.append(expression.substring(previousEnd));
                    expression = newExpression.toString(); // "#field1 = :value AND #field1 = :value2"
                }
            }
            request.setExpression(expression);
        }
    }

    @VisibleForTesting
    static void makePlaceholdersDistinct(String expression1, RequestWrapper expression2Request) {
        String expression2 = expression2Request.getExpression();
        expression2 = makePlaceholdersDistinct(expression1, expression2,
            "#field", expression2Request.getExpressionAttributeNames());
        expression2 = makePlaceholdersDistinct(expression1, expression2,
            ":value", expression2Request.getExpressionAttributeValues());
        expression2Request.setExpression(expression2);
    }

    private static <T> String makePlaceholdersDistinct(String primaryExpression,
                                                       String expression,
                                                       String newPlaceholderPrefix,
                                                       Map<String, T> placeholderToLiteralMap) {
        if (primaryExpression != null && expression != null && placeholderToLiteralMap != null) {
            Set<String> originalPlaceholders = new HashSet<>(placeholderToLiteralMap.keySet());
            for (String placeholder : originalPlaceholders) {
                Pattern pattern = Pattern.compile(placeholder + "(?=($|[^a-zA-Z0-9]))");
                // if the placeholder appears in the primary expression
                if (pattern.matcher(primaryExpression).find()) {
                    Matcher matcher = pattern.matcher(expression);
                    // and also in the expression being modified
                    if (matcher.find()) {
                        // then generate a new placeholder
                        String newPlaceholder = getNextPlaceholder(placeholderToLiteralMap, newPlaceholderPrefix);
                        // replace all instances of the original with the new placeholder
                        expression = matcher.replaceAll(newPlaceholder);
                        // and map the new placeholder to the corresponding literal
                        T literal = placeholderToLiteralMap.get(placeholder);
                        placeholderToLiteralMap.put(newPlaceholder, literal);
                    }
                }
            }
        }
        return expression;
    }

    /**
     * Finds the value in the right-hand side operand of an expression where the left-hand operator is a given field.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    @VisibleForTesting
    static Optional<String> findVirtualValuePlaceholder(String expression, String fieldPlaceholder) {
        if (expression == null) {
            return Optional.empty();
        }

        final Pattern p = Pattern.compile(fieldPlaceholder + " *" + COMPARISON_OP_PATTERN + " *(?<val>:[a-zA-Z0-9]+)");
        final Matcher m = p.matcher(expression);
        boolean found = m.find();
        return found ? Optional.of(m.group("val")) : Optional.empty();
    }

    private static class UpdateExpressionRequestWrapper extends AbstractRequestWrapper {

        UpdateExpressionRequestWrapper(UpdateItemRequest updateItemRequest) {
            super(updateItemRequest::getExpressionAttributeNames, updateItemRequest::setExpressionAttributeNames,
                updateItemRequest::getExpressionAttributeValues, updateItemRequest::setExpressionAttributeValues,
                updateItemRequest::getUpdateExpression, updateItemRequest::setUpdateExpression);
        }
    }

    @VisibleForTesting
    static class UpdateConditionExpressionRequestWrapper extends AbstractRequestWrapper {

        UpdateConditionExpressionRequestWrapper(UpdateItemRequest updateItemRequest) {
            super(updateItemRequest::getExpressionAttributeNames, updateItemRequest::setExpressionAttributeNames,
                updateItemRequest::getExpressionAttributeValues, updateItemRequest::setExpressionAttributeValues,
                updateItemRequest::getConditionExpression, updateItemRequest::setConditionExpression);
        }
    }

}