/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BETWEEN;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.grammar.ExpressionsBaseVisitor;
import com.salesforce.dynamodbv2.grammar.ExpressionsLexer;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.ComparatorContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.IdContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.KeyConditionContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.LiteralContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.SetActionContext;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RequestWrapper.AbstractRequestWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

abstract class AbstractConditionMapper implements ConditionMapper {

    private final DynamoTableDescription virtualTable;
    private final ItemMapper itemMapper;

    AbstractConditionMapper(DynamoTableDescription virtualTable, ItemMapper itemMapper) {
        this.virtualTable = virtualTable;
        this.itemMapper = itemMapper;
    }

    @Override
    public void applyForUpdate(UpdateItemRequest updateItemRequest) {
        if (updateItemRequest.getUpdateExpression() != null) {
            RequestWrapper request = new UpdateExpressionRequestWrapper(updateItemRequest);

            // parse update expression for the field values being set
            Map<String, AttributeValue> virtualSetActions = parseUpdateExpression(request,
                updateItemRequest.getConditionExpression());
            checkArgument(!virtualSetActions.isEmpty(), "Update expression needs at least one SET action");

            // validate no table primary key field is being updated, and that if one field in a secondary index is
            // being updated, then the other is as well
            validateFieldsCanBeUpdated(virtualSetActions.keySet());

            // map virtual field values to physical ones
            Map<String, AttributeValue> physicalSetActions = itemMapper.applyForWrite(virtualSetActions);

            // construct physical update SET expression
            List<String> clauses = physicalSetActions.entrySet().stream()
                .map(entry -> getUpdateSetClause(request, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
            request.setExpression("SET " + String.join(", ", clauses));
        }

        if (updateItemRequest.getConditionExpression() != null) {
            applyToFilterExpression(new UpdateConditionExpressionRequestWrapper(updateItemRequest));
        }
    }

    protected void validateFieldsCanBeUpdated(Set<String> allSetFields) {
        allSetFields.forEach(field -> {
            validateUpdatedFieldIsNotInPrimaryKey(field, virtualTable.getPrimaryKey().getHashKey());
            virtualTable.getPrimaryKey().getRangeKey().ifPresent(
                rk -> validateUpdatedFieldIsNotInPrimaryKey(field, rk));
        });
    }

    private void validateUpdatedFieldIsNotInPrimaryKey(String updatedField, String pkField) {
        if (updatedField.equals(pkField)) {
            throw new AmazonServiceException(
                String.format("Cannot update attribute %s. This attribute is part of the key", updatedField));
        }
    }

    private String getUpdateSetClause(RequestWrapper request, String field, AttributeValue value) {
        String fieldPlaceholder = MappingUtils.getNextFieldPlaceholder(request);
        request.putExpressionAttributeName(fieldPlaceholder, field);
        String valuePlaceholder = MappingUtils.getNextValuePlaceholder(request);
        request.putExpressionAttributeValue(valuePlaceholder, value);
        return fieldPlaceholder + " = " + valuePlaceholder;
    }

    @VisibleForTesting
    static Map<String, AttributeValue> parseUpdateExpression(RequestWrapper request,
                                                             String conditionExpression) {
        Set<String> conditionExprFieldPlaceholders = MappingUtils.getFieldPlaceholders(conditionExpression);
        Set<String> conditionExprValuePlaceholders = MappingUtils.getValuePlaceholders(conditionExpression);

        ExpressionsParser parser = getExpressionsParser(request.getExpression());
        UpdateExpressionVisitor visitor = new UpdateExpressionVisitor(request.getExpressionAttributeNames(),
            request.getExpressionAttributeValues(), conditionExprFieldPlaceholders, conditionExprValuePlaceholders);
        parser.updateExpression().accept(visitor);

        return visitor.setActions;
    }

    private static class UpdateExpressionVisitor extends ExpressionsBaseVisitor<Void> {

        private final Map<String, String> fieldPlaceholders;
        private final Map<String, AttributeValue> valuePlaceholders;
        private final Set<String> doNotRemoveFieldPlaceholders;
        private final Set<String> doNotRemoveValuePlaceholders;

        private Map<String, AttributeValue> setActions = new HashMap<>();

        UpdateExpressionVisitor(Map<String, String> fieldPlaceholders,
                                Map<String, AttributeValue> valuePlaceholders,
                                Set<String> doNotRemoveFieldPlaceholders,
                                Set<String> doNotRemoveValuePlaceholders) {
            this.fieldPlaceholders = fieldPlaceholders;
            this.valuePlaceholders = valuePlaceholders;
            this.doNotRemoveFieldPlaceholders = doNotRemoveFieldPlaceholders;
            this.doNotRemoveValuePlaceholders = doNotRemoveValuePlaceholders;
        }

        @Override
        public Void visitSetAction(SetActionContext setAction) {
            String fieldName = setAction.path().id().getText();
            String valuePlaceholder = setAction.setValue().literal().getText();
            if (fieldName.startsWith("#")) {
                fieldName = getAndRemovePlaceholderIfNeeded(fieldName, fieldPlaceholders,
                    doNotRemoveFieldPlaceholders);
            }
            if (setActions.containsKey(fieldName)) {
                throw new AmazonServiceException(
                    "Two document paths overlap with each other; must remove or rewrite one of these paths");
            }
            AttributeValue value = getAndRemovePlaceholderIfNeeded(valuePlaceholder, valuePlaceholders,
                doNotRemoveValuePlaceholders);
            setActions.put(fieldName, value);
            return null;
        }
    }

    static class UpdateExpressionRequestWrapper extends AbstractRequestWrapper {

        UpdateExpressionRequestWrapper(UpdateItemRequest updateItemRequest) {
            super(updateItemRequest::getExpressionAttributeNames, updateItemRequest::setExpressionAttributeNames,
                updateItemRequest::getExpressionAttributeValues, updateItemRequest::setExpressionAttributeValues,
                updateItemRequest::getUpdateExpression, updateItemRequest::setUpdateExpression);
        }
    }

    static class UpdateConditionExpressionRequestWrapper extends AbstractRequestWrapper {

        UpdateConditionExpressionRequestWrapper(UpdateItemRequest updateItemRequest) {
            super(updateItemRequest::getExpressionAttributeNames, updateItemRequest::setExpressionAttributeNames,
                updateItemRequest::getExpressionAttributeValues, updateItemRequest::setExpressionAttributeValues,
                updateItemRequest::getConditionExpression, updateItemRequest::setConditionExpression);
        }
    }

    @Override
    public void applyToKeyCondition(RequestWrapper request, RequestIndex requestIndex, String filterExpression) {
        // parse expression for the virtual HK value, and the RK condition if it exists
        ExpressionsParser parser = getExpressionsParser(request.getExpression());
        Set<String> filterExprValuePlaceholders = MappingUtils.getValuePlaceholders(filterExpression);
        KeyConditionExpressionVisitor visitor = new KeyConditionExpressionVisitor(requestIndex.getVirtualPk(),
            request.getExpressionAttributeNames(), request.getExpressionAttributeValues(),
            filterExprValuePlaceholders);
        parser.keyConditionExpression().accept(visitor);
        checkNotNull(visitor.hkValue, "Key condition does not specify hash key");

        // map virtual expression & values to physical expression & values
        String physicalExpression = mapKeyConditionExpression(request, requestIndex, visitor.hkValue,
            visitor.rkCondition);
        request.setExpression(physicalExpression);
    }

    abstract String mapKeyConditionExpression(RequestWrapper request, RequestIndex requestIndex,
                                              AttributeValue virtualHkValue,
                                              Optional<KeyFieldCondition> virtualRkCondition);

    private static class KeyConditionExpressionVisitor extends ExpressionsBaseVisitor<Void> {

        private final PrimaryKey primaryKey;
        private final Map<String, String> fieldPlaceholders;
        private final Map<String, AttributeValue> valuePlaceholders;
        private final Set<String> doNotRemoveValuePlaceholders;

        // output
        private AttributeValue hkValue = null;
        private Optional<KeyFieldCondition> rkCondition = Optional.empty();

        KeyConditionExpressionVisitor(PrimaryKey primaryKey, Map<String, String> fieldPlaceholders,
                                      Map<String, AttributeValue> valuePlaceholders,
                                      Set<String> doNotRemoveValuePlaceholders) {
            this.primaryKey = primaryKey;
            this.fieldPlaceholders = fieldPlaceholders;
            this.valuePlaceholders = valuePlaceholders;
            this.doNotRemoveValuePlaceholders = doNotRemoveValuePlaceholders;
        }

        @Override
        public Void visitKeyCondition(KeyConditionContext keyCondition) {
            IdContext id = keyCondition.id();
            if (id != null) {
                String fieldName = keyCondition.id().getText();
                if (fieldName.startsWith("#")) {
                    // if it's a field placeholder, remove it -- we'll generated a new one for translated expression.
                    // (we don't need to worry about the field placeholder also being used in the filter expression,
                    // since the filter expression can contain only non-key fields.)
                    fieldName = removeFieldPlaceholder(fieldName);
                }

                if (primaryKey.getHashKey().equals(fieldName)) {
                    checkArgument(hkValue == null, "Multiple hash key conditions");
                    ComparatorContext comparator = keyCondition.comparator();
                    checkArgument(comparator != null && comparator.getText().equals("="),
                        "Hash key condition must have operator '='");
                    String valuePlaceholder = keyCondition.literal(0).getText();
                    hkValue = getAndRemoveValuePlaceholderIfNeeded(valuePlaceholder);
                } else if (primaryKey.getRangeKey().isPresent() && primaryKey.getRangeKey().get().equals(fieldName)) {
                    checkArgument(rkCondition.isEmpty(), "Multiple range key conditions");
                    ComparisonOperator op = getRangeKeyComparisonOp(keyCondition);
                    List<AttributeValue> values = keyCondition.literal().stream()
                        .map(LiteralContext::getText)
                        .map(this::getAndRemoveValuePlaceholderIfNeeded)
                        .collect(Collectors.toList());
                    rkCondition = Optional.of(new KeyFieldCondition(op, values));
                } else {
                    throw new IllegalArgumentException("Key condition expression contains non-key field: " + fieldName);
                }
            }
            return visitChildren(keyCondition);
        }

        private String removeFieldPlaceholder(String placeholder) {
            String literal = null;
            if (fieldPlaceholders != null) {
                literal = fieldPlaceholders.remove(placeholder);
            }
            checkArgument(literal != null, "Referenced attribute name does not exist: %s", placeholder);
            return literal;
        }

        private AttributeValue getAndRemoveValuePlaceholderIfNeeded(String placeholder) {
            return getAndRemovePlaceholderIfNeeded(placeholder, valuePlaceholders, doNotRemoveValuePlaceholders);
        }

        private ComparisonOperator getRangeKeyComparisonOp(KeyConditionContext rangeKeyCondition) {
            ComparatorContext singelValueComparator = rangeKeyCondition.comparator();
            if (singelValueComparator != null) {
                return getSingleRangeKeyValueComparisonOp(singelValueComparator.getText());
            } else {
                checkArgument(rangeKeyCondition.BETWEEN() != null,
                    "Invalid range key condition: %s", rangeKeyCondition.getText());
                return BETWEEN;
            }
        }

        private ComparisonOperator getSingleRangeKeyValueComparisonOp(String comparator) {
            switch (comparator) {
                case "=":
                    return EQ;
                case ">":
                    return GT;
                case ">=":
                    return GE;
                case "<":
                    return LT;
                case "<=":
                    return LE;
                default:
                    throw new IllegalArgumentException("Invalid range key comparator: " + comparator);
            }
        }
    }

    static class KeyFieldCondition {

        private final ComparisonOperator operator;
        private final List<AttributeValue> values;

        KeyFieldCondition(ComparisonOperator operator, List<AttributeValue> values) {
            checkArgument(values.size() == (operator == BETWEEN ? 2 : 1));
            this.operator = operator;
            this.values = values;
        }

        ComparisonOperator getOperator() {
            return operator;
        }

        List<AttributeValue> getValues() {
            return values;
        }
    }

    private static <T> T getAndRemovePlaceholderIfNeeded(String placeholder, Map<String, T> allPlaceholders,
                                                         Set<String> doNotRemovePlaceholders) {
        T result = null;
        if (allPlaceholders != null) {
            // keep the placeholder if it's also in the filter expression; otherwise remove it
            result = doNotRemovePlaceholders.contains(placeholder)
                ? allPlaceholders.get(placeholder)
                : allPlaceholders.remove(placeholder);
        }
        checkArgument(result != null, "Referenced placeholder does not exist: %s", placeholder);
        return result;
    }

    private static class ThrowingAntlrErrorListener extends BaseErrorListener {

        private final String expression;

        ThrowingAntlrErrorListener(String expression) {
            this.expression = expression;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            throw new IllegalArgumentException("Syntax error while parsing expression \"" + expression
                + "\". Line " + line + ":" + charPositionInLine + " " + msg);
        }
    }

    private static ExpressionsParser getExpressionsParser(String expression) {
        ExpressionsLexer lexer = new ExpressionsLexer(CharStreams.fromString(expression));
        ExpressionsParser parser = new ExpressionsParser(new CommonTokenStream(lexer));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ThrowingAntlrErrorListener(expression));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser;
    }

}
