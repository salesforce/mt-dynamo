package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextFieldPlaceholder;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextValuePlaceholder;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
class RandomPartitioningConditionMapper extends AbstractConditionMapper {

    static final String NAME_PLACEHOLDER = "#___name___";
    private static final String COMPARISON_OP_PATTERN = "(=|<>|<|<=|>|>=)";

    private final RandomPartitioningTableMapping tableMapping;
    private final FieldMapper fieldMapper;

    RandomPartitioningConditionMapper(RandomPartitioningTableMapping tableMapping, FieldMapper fieldMapper) {
        super(tableMapping.getVirtualTable(), tableMapping.getItemMapper());
        this.tableMapping = tableMapping;
        this.fieldMapper = fieldMapper;
    }

    @Override
    String mapKeyConditionExpression(RequestWrapper request, RequestIndex requestIndex, AttributeValue virtualHkValue,
                                     Optional<KeyFieldCondition> virtualRkCondition) {
        List<FieldMapping> fieldMappings = requestIndex.getVirtualSecondaryIndex().isPresent()
            ? tableMapping.getIndexPrimaryKeyFieldMappings(requestIndex.getVirtualSecondaryIndex().get())
            : tableMapping.getTablePrimaryKeyFieldMappings();
        FieldMapping hkFieldMapping = fieldMappings.get(0);
        Optional<FieldMapping> rkFieldMapping = virtualRkCondition.map(r -> fieldMappings.get(1));

        String hkFieldPlaceholder = getNextFieldPlaceholder(request);
        request.putExpressionAttributeName(hkFieldPlaceholder, requestIndex.getPhysicalPk().getHashKey());
        String hkValuePlaceholder = getNextValuePlaceholder(request);
        request.putExpressionAttributeValue(hkValuePlaceholder, applyFieldMapping(hkFieldMapping, virtualHkValue));
        String expression = hkFieldPlaceholder + " = " + hkValuePlaceholder;

        if (virtualRkCondition.isPresent()) {
            String rkFieldPlaceholder = getNextFieldPlaceholder(request);
            request.putExpressionAttributeName(rkFieldPlaceholder, requestIndex.getPhysicalPk().getRangeKey().get());
            List<String> rkValuePlaceholders = new ArrayList<>();
            for (AttributeValue value : virtualRkCondition.get().getValues()) {
                String valuePlaceholder = getNextValuePlaceholder(request);
                rkValuePlaceholders.add(valuePlaceholder);
                request.putExpressionAttributeValue(valuePlaceholder, applyFieldMapping(rkFieldMapping.get(), value));
            }
            expression += " AND " + getRangeKeyCondition(virtualRkCondition.get().getOperator(), rkFieldPlaceholder,
                rkValuePlaceholders);
        }

        return expression;
    }

    private AttributeValue applyFieldMapping(FieldMapping fieldMapping, AttributeValue value) {
        return fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, value) : value;
    }

    private String getRangeKeyCondition(ComparisonOperator operator, String fieldPlaceholder,
                                        List<String> valuePlaceholders) {
        switch (operator) {
            case EQ:
                return fieldPlaceholder + " = " + valuePlaceholders.get(0);
            case LT:
                return fieldPlaceholder + " < " + valuePlaceholders.get(0);
            case LE:
                return fieldPlaceholder + " <= " + valuePlaceholders.get(0);
            case GT:
                return fieldPlaceholder + " > " + valuePlaceholders.get(0);
            case GE:
                return fieldPlaceholder + " >= " + valuePlaceholders.get(0);
            case BETWEEN:
                return fieldPlaceholder + " BETWEEN " + valuePlaceholders.get(0) + " AND " + valuePlaceholders.get(1);
            default:
                throw new IllegalArgumentException(
                    "Comparison operator not supported in query range key condition: " + operator.name());
        }
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