package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
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
import javax.annotation.Nullable;

/**
 * {@link ConditionMapper} implementation for shared tables using random partitioning.
 *
 * @author msgroi
 */
class RandomPartitioningConditionMapper implements ConditionMapper {

    static final String NAME_PLACEHOLDER = "#___name___";
    private static final String COMPARISON_OP_PATTERN = "(=|<>|<|<=|>|>=)";
    private static final Pattern ATTR_NAME_PLACEHOLDER_PATTERN = Pattern.compile("#[a-zA-Z0-9]+");
    private static final Pattern ATTR_VALUE_PLACEHOLDER_PATTERN = Pattern.compile(":[a-zA-Z0-9]+");

    private final RandomPartitioningTableMapping tableMapping;
    private final FieldMapper fieldMapper;

    RandomPartitioningConditionMapper(RandomPartitioningTableMapping tableMapping, FieldMapper fieldMapper) {
        this.tableMapping = tableMapping;
        this.fieldMapper = fieldMapper;
    }

    @Override
    public void applyForUpdate(RequestWrapper request) {
        String updateExpression = request.getPrimaryExpression();
        String conditionExpression = request.getFilterExpression();
        Set<String> virtualFieldNames = tableMapping.getAllMappingsPerField().keySet();

        updateExpression = convertFieldNameLiteralsToExpressionNames(updateExpression, request, virtualFieldNames);
        validateNotUpdatingTablePrimaryKeyFields(updateExpression, request);

        if (conditionExpression != null) {
            conditionExpression = convertFieldNameLiteralsToExpressionNames(conditionExpression, request,
                virtualFieldNames);
            conditionExpression = makePlaceholdersDistinct(updateExpression, conditionExpression, request);
            request.setFilterExpression(conditionExpression);
            applyToFilterExpression(request, false);
        }

        for (Entry<String, List<FieldMapping>> entry : tableMapping.getAllMappingsPerField().entrySet()) {
            updateExpression = mapFieldInUpdateExpression(updateExpression, request, entry.getKey(), entry.getValue());
        }
        request.setPrimaryExpression(updateExpression);
    }

    @Override
    public void applyToKeyCondition(RequestWrapper request, @Nullable DynamoSecondaryIndex virtualSecondaryIndex) {
        Collection<FieldMapping> fieldMappings = virtualSecondaryIndex == null
            ? tableMapping.getTablePrimaryKeyFieldMappings()
            : tableMapping.getIndexPrimaryKeyFieldMappings(virtualSecondaryIndex);

        String expression = request.getPrimaryExpression();
        expression = mapFieldsInConditionExpression(expression, request, fieldMappings);
        request.setPrimaryExpression(expression);
    }

    @Override
    public void applyToFilterExpression(RequestWrapper request, boolean isPrimaryExpression) {
        String expression = isPrimaryExpression ? request.getPrimaryExpression() : request.getFilterExpression();

        // for conditional / filter expressions, for each indexed field, we only need to use one of its field mappings,
        // since any of the possible physical field comparison expressions are true IFF virtual field comparison is true
        List<FieldMapping> fieldMappings = new ArrayList<>(tableMapping.getAllMappingsPerField().size());
        tableMapping.getAllMappingsPerField().forEach((virtualField, mappings) -> fieldMappings.add(mappings.get(0)));

        expression = mapFieldsInConditionExpression(expression, request, fieldMappings);

        if (isPrimaryExpression) {
            request.setPrimaryExpression(expression);
        } else {
            request.setFilterExpression(expression);
        }
    }

    /**
     * Used by applyToKeyCondition() and applyToFilterExpression() to apply the given field mappings to a
     * condition/filter expression.
     */
    private String mapFieldsInConditionExpression(String expression,
                                                  RequestWrapper request,
                                                  Collection<FieldMapping> fieldMappings) {
        Set<String> virtualFieldNames = fieldMappings.stream()
            .map(m -> m.getSource().getName())
            .collect(Collectors.toSet());
        String newExpression = convertFieldNameLiteralsToExpressionNames(expression, request, virtualFieldNames);

        fieldMappings.forEach(fieldMapping -> mapFieldInConditionExpression(newExpression, request, fieldMapping));
        return newExpression;
    }

    /**
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the mapping, and sets the physical name of the field to that of the target field.
     *
     * <p>Comments show expected variable values with a sample set of inputs.
     */
    @VisibleForTesting
    void mapFieldInConditionExpression(String expression, // "#field1 = :value"
                                       RequestWrapper request,
                                       FieldMapping fieldMapping) {
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
        }
    }

    @VisibleForTesting
    String mapFieldInUpdateExpression(String expression, // "#field1 = :value"
                                      RequestWrapper request,
                                      String virtualField, // "virtualHk"
                                      List<FieldMapping> fieldMappings) {
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
                            : getNextPlaceholder(request.getExpressionAttributeNames(), "#field");
                        request.putExpressionAttributeName(physicalFieldPlaceholder, physicalField);

                        AttributeValue physicalValue = fieldMapping.isContextAware()
                            ? fieldMapper.apply(fieldMapping, virtualValue) // {S: ctx.virtualTable.hkValue,}
                            : virtualValue;
                        String physicalValuePlaceholder = equalsClauses.isEmpty()
                            ? virtualValuePlaceholder
                            : getNextPlaceholder(request.getExpressionAttributeValues(), ":value");
                        request.putExpressionAttributeValue(physicalValuePlaceholder, physicalValue);

                        equalsClauses.add(physicalFieldPlaceholder + " = " + physicalValuePlaceholder);
                    }

                    // possibly fan out to assigning multiple fields for SET
                    String physicalClause = String.join(", ", equalsClauses);
                    expression = expression.replaceAll(
                        virtualFieldPlaceholder + " *= *" + virtualValuePlaceholder + "\\b", physicalClause);
                }
            }
        }
        return expression;
    }

    private void validateNotUpdatingTablePrimaryKeyFields(String updateExpression, RequestWrapper request) {
        if (updateExpression != null) {
            PrimaryKey primaryKey = tableMapping.getVirtualTable().getPrimaryKey();
            Set<String> tablePrimaryKeyFields = new HashSet<>();
            tablePrimaryKeyFields.add(primaryKey.getHashKey());
            primaryKey.getRangeKey().ifPresent(tablePrimaryKeyFields::add);

            Matcher m = ATTR_NAME_PLACEHOLDER_PATTERN.matcher(updateExpression);
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
    static String convertFieldNameLiteralsToExpressionNames(
        String expression, // "literal1 = :value AND literal1 = :value2"
        RequestWrapper request,
        Collection<String> virtualFieldNames // ["literal1", "literal2"]
    ) {
        if (expression != null) {
            for (String virtualFieldName : virtualFieldNames) {
                Pattern p = Pattern.compile("(^| )((?<comparison>" + virtualFieldName + ") *" + COMPARISON_OP_PATTERN
                    + "|attribute_exists\\((?<exists>" + virtualFieldName + ")\\)"
                    + "|attribute_not_exists\\((?<notexists>" + virtualFieldName + ")\\))");
                Matcher m = p.matcher(expression);
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
                    String placeholder =
                        getNextPlaceholder(request.getExpressionAttributeNames(), "#field");
                    request.putExpressionAttributeName(placeholder, virtualFieldName); // #field1 -> literal1

                    StringBuilder newExpression = new StringBuilder();
                    Iterator<Integer> startsIterator = starts.iterator();
                    int previousEnd = 0;
                    do {
                        int start = startsIterator.next();
                        newExpression.append(expression.substring(previousEnd, start)).append(placeholder);
                        previousEnd = start + virtualFieldName.length();
                    } while (startsIterator.hasNext());
                    newExpression.append(expression.substring(previousEnd));
                    expression = newExpression.toString(); // "#field1 = :value AND #field1 = :value2"
                }
            }
        }
        return expression;
    }

    @VisibleForTesting
    static String makePlaceholdersDistinct(String primaryExpression, String expression, RequestWrapper request) {
        expression = makePlaceholdersDistinct(primaryExpression, expression, ATTR_NAME_PLACEHOLDER_PATTERN, "#field",
            request.getExpressionAttributeNames());
        expression = makePlaceholdersDistinct(primaryExpression, expression, ATTR_VALUE_PLACEHOLDER_PATTERN, ":value",
            request.getExpressionAttributeValues());
        return expression;
    }

    private static <T> String makePlaceholdersDistinct(String primaryExpression,
                                                       String expression,
                                                       Pattern placeholderPattern,
                                                       String newPlaceholderPrefix,
                                                       Map<String, T> placeholderToLiteralMap) {
        if (expression != null) {
            Matcher m = placeholderPattern.matcher(expression);
            Set<String> overlappingPlaceholders = new HashSet<>();
            while (m.find()) {
                String placeholder = m.group();
                if (primaryExpression.contains(placeholder)) { // this check may have false positives but that's okay
                    overlappingPlaceholders.add(placeholder);
                }
            }
            for (String placeholder : overlappingPlaceholders) {
                T literal = placeholderToLiteralMap.get(placeholder);
                String newPlaceholder = getNextPlaceholder(placeholderToLiteralMap, newPlaceholderPrefix);
                expression = expression.replaceAll(placeholder + "\\b", newPlaceholder);
                placeholderToLiteralMap.put(newPlaceholder, literal);
            }
        }
        return expression;
    }

    /**
     * Generates a field name placeholder starting with '#field' and suffixed with an incrementing number skipping
     * any reference that already exists in the expressionAttributeNames map.
     */
    @VisibleForTesting
    static String getNextPlaceholder(Map<String, ?> existingPlaceholders, String prefix) {
        int counter = 1;
        String placeholderCandidate = prefix + counter;
        while (existingPlaceholders != null && existingPlaceholders.containsKey(placeholderCandidate)) {
            counter++;
            placeholderCandidate = prefix + counter;
        }
        return placeholderCandidate;
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

}