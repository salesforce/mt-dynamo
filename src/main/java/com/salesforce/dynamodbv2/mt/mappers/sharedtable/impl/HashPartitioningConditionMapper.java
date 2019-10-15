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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.primitives.UnsignedBytes;
import com.salesforce.dynamodbv2.grammar.ExpressionsBaseVisitor;
import com.salesforce.dynamodbv2.grammar.ExpressionsLexer;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.ComparatorContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.IdContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.KeyConditionPartContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.LiteralContext;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.PhysicalRangeKeyBytesConverter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

class HashPartitioningConditionMapper implements ConditionMapper {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private final HashPartitioningKeyMapper keyMapper;

    HashPartitioningConditionMapper(HashPartitioningKeyMapper keyMapper) {
        this.keyMapper = keyMapper;
    }

    @Override
    public void applyForUpdate(UpdateItemRequest updateItemRequest) {
        // TODO
    }

    @Override
    public void applyToFilterExpression(RequestWrapper request) {
        // no-op, since we keep all original virtual fields
    }

    @Override
    public void applyToKeyCondition(RequestWrapper request, RequestIndex requestIndex) {
        // parse expression for the virtual HK value, and the RK condition if it exists
        ExpressionsParser parser = getExpressionsParser(request.getExpression());
        KeyConditionExpressionVisitor visitor = new KeyConditionExpressionVisitor(requestIndex.getVirtualPk(),
            request.getExpressionAttributeNames(), request.getExpressionAttributeValues());
        parser.keyConditionExpression().accept(visitor);
        checkNotNull(visitor.hkValue, "Key condition does not specify hash key");

        // map virtual expression & values to physical expression & values
        KeyConditionExpressionMapper expressionMapper = new KeyConditionExpressionMapper(requestIndex.getVirtualPk(),
            requestIndex.getPhysicalPk(), visitor.hkValue, visitor.rkCondition);
        String physicalExpression = expressionMapper.mapKeyConditionExpression(request);
        request.setExpression(physicalExpression);
    }

    /**
     * Maps a virtual query key condition expression to a physical one. Summary:
     * <ul>
     *     <li>"#vhk = :vhk", no vrk ==> "#prk = :vhk“
     *     <li>"#vhk = :vhk and #vrk = :vrk" ==> "#prk = :vhk+vrk“
     *     <li>"#vhk = :vhk", has vrk ==> "#prk BETWEEN :vhk AND :vhk+FFFFFF)"
     *     <li>"#vhk = :vhk and #vrk < :vrk" ==> "#prk BETWEEN :vhk AND :vhk+(vrk-1)+FFFFFF“
     *     <li>"#vhk = :vhk and #vrk <= :vrk" ==> "#prk BETWEEN :vhk AND :vhk+vrk“
     *     <li>"#vhk = :vhk and #vrk > :vrk" ==> "#prk BETWEEN :vhk+vrk+0 AND :vhk+FFFFFF“
     *     <li>"#vhk = :vhk and #vrk >= :vrk" ==> "#prk BETWEEN :vhk+vrk AND :vhk+FFFFFF“
     *     <li>"#vhk = :vhk and #vrk BETWEEN :vrk1 AND :vrk2" ==> "#prk BETWEEN :vhk+vrk1 AND :vhk+vrk2“
     * </ul>
     */
    private class KeyConditionExpressionMapper {

        private final PrimaryKey virtualPk;
        private final PrimaryKey physicalPk;
        private final AttributeValue virtualHkValue;
        private final Optional<KeyFieldCondition> virtualRkCondition;

        KeyConditionExpressionMapper(PrimaryKey virtualPk, PrimaryKey physicalPk,
                                     AttributeValue virtualHkValue, Optional<KeyFieldCondition> virtualRkCondition) {
            this.virtualPk = virtualPk;
            this.physicalPk = physicalPk;
            this.virtualHkValue = virtualHkValue;
            this.virtualRkCondition = virtualRkCondition;
        }

        String mapKeyConditionExpression(RequestWrapper request) {
            String physicalHkCondition = getHashKeyCondition(request);
            String physicalRkCondition = getRangeKeyCondition(request);
            return physicalHkCondition + " AND " + physicalRkCondition;
        }

        private String getHashKeyCondition(RequestWrapper request) {
            AttributeValue physicalHkValue = keyMapper.toPhysicalHashKey(virtualPk.getHashKeyType(), virtualHkValue);
            String fieldPlaceholder = setFieldPlaceholder(request, physicalPk.getHashKey());
            String valuePlaceholder = setValuePlaceholder(request, physicalHkValue);
            return fieldPlaceholder + " = " + valuePlaceholder;
        }

        private String getRangeKeyCondition(RequestWrapper request) {
            // "#vhk = :vhk", no virtual RK ==> "#prk = :vhk“
            if (virtualPk.getRangeKey().isEmpty()) {
                return getRangeKeyEquals(request, keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue));
            }

            // "#vhk = :vhk", has virtual RK ==> "#prk BETWEEN vhk AND :vhk+FFFFFF"
            if (virtualRkCondition.isEmpty()) {
                AttributeValue lower = keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY);
                AttributeValue upper = keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY, true);
                return getRangeKeyBetween(request, lower, upper);
            }

            ComparisonOperator comparisonOperator = virtualRkCondition.get().operator;
            List<AttributeValue> vrkValues = virtualRkCondition.get().values;

            // "#vhk = :vhk and #vrk = :vrk" ==> "#prk = :vhk+vrk“
            if (comparisonOperator == EQ) {
                AttributeValue value = keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(0));
                return getRangeKeyEquals(request, value);
            }

            BetweenRange betweenRange = getBetweenRange(comparisonOperator, vrkValues);
            return getRangeKeyBetween(request, betweenRange.lower, betweenRange.upper);
        }

        private BetweenRange getBetweenRange(ComparisonOperator comparisonOperator, List<AttributeValue> vrkValues) {
            ScalarAttributeType virtualRkType = virtualPk.getRangeKeyType().get();
            switch (comparisonOperator) {
                case LT:
                    // "#vhk = :vhk and #vrk < :vrk" ==> "#prk BETWEEN :vhk AND :vhk+(vrk-1)+FFFFFF“
                    ByteBuffer rkBytes = PhysicalRangeKeyBytesConverter.toBytes(virtualRkType, vrkValues.get(0));
                    byte[] rkByteArray = Arrays.copyOf(rkBytes.array(), rkBytes.limit());
                    subtractOneFromByteArray(rkByteArray);
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY),
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, rkByteArray, true));
                case LE:
                    // "#vhk = :vhk and #vrk <= :vrk" ==> "#prk BETWEEN :vhk AND :vhk+vrk“
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY),
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(0)));
                case GT:
                    // #vhk = :vhk and #vrk > :vrk" ==> "#prk BETWEEN :vhk+vrk+0 AND :vhk+FFFFFF“
                    rkBytes = PhysicalRangeKeyBytesConverter.toBytes(virtualRkType, vrkValues.get(0));
                    byte[] lowerBoundBytes = Arrays.copyOf(rkBytes.array(), rkBytes.limit() + 1);
                    lowerBoundBytes[lowerBoundBytes.length - 1] = 0;
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, lowerBoundBytes),
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY, true));
                case GE:
                    // "#vhk = :vhk and #vrk >= :vrk" ==> "#prk BETWEEN :vhk+vrk AND :vhk+FFFFFF“
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(0)),
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, EMPTY_BYTE_ARRAY, true));
                case BETWEEN:
                    // "#vhk = :vhk and #vrk BETWEEN :vrk1 AND :vrk2" ==> "#prk BETWEEN :vhk+vrk1 AND :vhk+vrk2“
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(0)),
                        keyMapper.toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(1)));
                default:
                    throw new IllegalArgumentException(
                        "Comparison operator not supported in query range key condition: " + comparisonOperator.name());
            }
        }

        /**
         * Subtract 1 from the byte array. That is, if the least significant byte > 1, then set that byte to be 1 less.
         * Otherwise, set the byte to the max unsigned byte and repeat with the next least significant byte.
         * Example: {1, 2, 3, 0, 0} => {1, 2, 2, -1, -1}
         */
        private void subtractOneFromByteArray(byte[] bytes) {
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] == 0) {
                    bytes[i] = UnsignedBytes.MAX_VALUE;
                } else {
                    bytes[i] = (byte)(bytes[i] - 1);
                    return;
                }
            }
            throw new IllegalArgumentException("Input byte array should not all by zeros");
        }

        private String getRangeKeyEquals(RequestWrapper request, AttributeValue value) {
            String fieldPlaceholder = setFieldPlaceholder(request, physicalPk.getRangeKey().get());
            String valuePlaceholder = setValuePlaceholder(request, value);
            return fieldPlaceholder + " = " + valuePlaceholder;
        }

        private String getRangeKeyBetween(RequestWrapper request, AttributeValue lower, AttributeValue upper) {
            String fieldPlaceholder = setFieldPlaceholder(request, physicalPk.getRangeKey().get());
            String lowerPlaceholder = setValuePlaceholder(request, lower);
            String upperPlaceholder = setValuePlaceholder(request, upper);
            return String.format("%s BETWEEN %s AND %s", fieldPlaceholder, lowerPlaceholder,
                upperPlaceholder);
        }

        private String setValuePlaceholder(RequestWrapper request, AttributeValue value) {
            String placeholder = MappingUtils.getNextValuePlaceholder(request);
            request.putExpressionAttributeValue(placeholder, value);
            return placeholder;
        }

        private String setFieldPlaceholder(RequestWrapper request, String field) {
            String placeholder = MappingUtils.getNextFieldPlaceholder(request);
            request.putExpressionAttributeName(placeholder, field);
            return placeholder;
        }
    }

    private static class BetweenRange {

        private final AttributeValue lower;
        private final AttributeValue upper;

        BetweenRange(AttributeValue lower, AttributeValue upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private static class KeyConditionExpressionVisitor extends ExpressionsBaseVisitor<Void> {

        private final PrimaryKey primaryKey;
        private final Map<String, String> fieldPlaceholders;
        private final Map<String, AttributeValue> valuePlaceholders;

        // output
        private AttributeValue hkValue = null;
        private Optional<KeyFieldCondition> rkCondition = Optional.empty();

        KeyConditionExpressionVisitor(PrimaryKey primaryKey, Map<String, String> fieldPlaceholders,
                                      Map<String, AttributeValue> valuePlaceholders) {
            this.primaryKey = primaryKey;
            this.fieldPlaceholders = fieldPlaceholders;
            this.valuePlaceholders = valuePlaceholders;
        }

        @Override
        public Void visitKeyConditionPart(KeyConditionPartContext keyConditionPart) {
            IdContext id = keyConditionPart.id();
            if (id != null) {
                String fieldName = keyConditionPart.id().getText();
                if (fieldName.startsWith("#")) {
                    // if it's a field placeholder, remove it -- we'll generated a new one for translated expression
                    fieldName = removeFieldPlaceholder(fieldName);
                }

                if (primaryKey.getHashKey().equals(fieldName)) {
                    checkArgument(hkValue == null, "Multiple hash key conditions");
                    ComparatorContext comparator = keyConditionPart.comparator();
                    checkArgument(comparator != null && comparator.getText().equals("="),
                        "Hash key condition must have operator '='");
                    String valuePlaceholder = keyConditionPart.literal(0).getText();
                    hkValue = removeValuePlaceholder(valuePlaceholder);
                } else if (primaryKey.getRangeKey().isPresent() && primaryKey.getRangeKey().get().equals(fieldName)) {
                    checkArgument(rkCondition.isEmpty(), "Multiple range key conditions");
                    ComparatorContext comparator = keyConditionPart.comparator();
                    ComparisonOperator op = comparator != null ? getComparatorType(comparator.getText()) : BETWEEN;
                    List<AttributeValue> values = keyConditionPart.literal().stream()
                        .map(LiteralContext::getText)
                        .map(this::removeValuePlaceholder)
                        .collect(Collectors.toList());
                    rkCondition = Optional.of(new KeyFieldCondition(op, values));
                } else {
                    throw new IllegalArgumentException("Key condition expression contains non-key field: " + fieldName);
                }
            }
            return visitChildren(keyConditionPart);
        }

        private AttributeValue removeValuePlaceholder(String placeholder) {
            AttributeValue value = null;
            if (valuePlaceholders != null) {
                value = valuePlaceholders.remove(placeholder);
            }
            checkArgument(value != null, "Referenced attribute value does not exist: %s", placeholder);
            return value;
        }

        private String removeFieldPlaceholder(String placeholder) {
            String literal = null;
            if (fieldPlaceholders != null) {
                literal = fieldPlaceholders.remove(placeholder);
            }
            checkArgument(literal != null, "Referenced attribute name does not exist: %s", placeholder);
            return literal;
        }
    }

    private static ComparisonOperator getComparatorType(String comparator) {
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

    private static class KeyFieldCondition {

        private final ComparisonOperator operator;
        private final List<AttributeValue> values;

        KeyFieldCondition(ComparisonOperator operator, List<AttributeValue> values) {
            checkArgument(values.size() == (operator == BETWEEN ? 2 : 1));
            this.operator = operator;
            this.values = values;
        }
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

    private ExpressionsParser getExpressionsParser(String expression) {
        ExpressionsLexer lexer = new ExpressionsLexer(CharStreams.fromString(expression));
        ExpressionsParser parser = new ExpressionsParser(new CommonTokenStream(lexer));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ThrowingAntlrErrorListener(expression));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser;
    }

}
