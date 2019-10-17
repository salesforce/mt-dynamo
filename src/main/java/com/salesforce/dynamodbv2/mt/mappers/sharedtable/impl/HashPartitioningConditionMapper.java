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
import java.util.Arrays;
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
    public void applyToKeyCondition(RequestWrapper request, RequestIndex requestIndex, String filterExpression) {
        // parse expression for the virtual HK value, and the RK condition if it exists
        ExpressionsParser parser = getExpressionsParser(request.getExpression());
        Set<String> valuePlaceholders = MappingUtils.getValuePlaceholders(filterExpression);
        KeyConditionExpressionVisitor visitor = new KeyConditionExpressionVisitor(requestIndex.getVirtualPk(),
            request.getExpressionAttributeNames(), request.getExpressionAttributeValues(), valuePlaceholders);
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
     *     <li>"#vhk = :vhk and #vrk < :vrk" ==> "#prk BETWEEN :vhk AND :vhk+maxValueLessThan(vrk)“
     *     <li>"#vhk = :vhk and #vrk <= :vrk" ==> "#prk BETWEEN :vhk AND :vhk+vrk“
     *     <li>"#vhk = :vhk and #vrk > :vrk" ==> "#prk BETWEEN :vhk+minValueGreaterThan(vrk) AND :vhk+FFFFFF“
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
                byte[] vhk = PhysicalRangeKeyBytesConverter.toByteArray(virtualPk.getHashKeyType(), virtualHkValue);
                AttributeValue lower = keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY);
                AttributeValue upper = keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true);
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
            byte[] vhk = PhysicalRangeKeyBytesConverter.toByteArray(virtualPk.getHashKeyType(), virtualHkValue);
            ScalarAttributeType virtualRkType = virtualPk.getRangeKeyType().get();
            byte[] vrk = PhysicalRangeKeyBytesConverter.toByteArray(virtualRkType, vrkValues.get(0));
            switch (comparisonOperator) {
                case LT:
                    // "#vhk = :vhk and #vrk < :vrk" ==> "#prk BETWEEN :vhk AND :vhk+maxValueLessThan(vrk)“
                    // (for the upper bound, we compute the largest value that's smaller than the specified virtual RK
                    // RK value, since BETWEEN is inclusive)
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY),
                        getMaxValueLessThan(vhk, vrk));
                case LE:
                    // "#vhk = :vhk and #vrk <= :vrk" ==> "#prk BETWEEN :vhk AND :vhk+vrk“
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY),
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, vrk));
                case GT:
                    // #vhk = :vhk and #vrk > :vrk" ==> "#prk BETWEEN :vhk+minValueGreaterThan(vrk) AND :vhk+FFFFFF“
                    // (for the lower bound, we compute the smallest value that's greater than the specified virtual RK
                    // RK value, since BETWEEN is inclusive)
                    return new BetweenRange(
                        getMinValueGreaterThan(vhk, vrk),
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true));
                case GE:
                    // "#vhk = :vhk and #vrk >= :vrk" ==> "#prk BETWEEN :vhk+vrk AND :vhk+FFFFFF“
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, vrk),
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true));
                case BETWEEN:
                    // "#vhk = :vhk and #vrk BETWEEN :vrk1 AND :vrk2" ==> "#prk BETWEEN :vhk+vrk1 AND :vhk+vrk2“
                    byte[] vrkUpper = PhysicalRangeKeyBytesConverter.toByteArray(virtualRkType, vrkValues.get(1));
                    return new BetweenRange(
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, vrk),
                        keyMapper.toPhysicalRangeKey(virtualPk, vhk, vrkUpper));
                default:
                    throw new IllegalArgumentException(
                        "Comparison operator not supported in query range key condition: " + comparisonOperator.name());
            }
        }

        /**
         * Returns the largest VHK+VRK' value that is less than the VHK+VRK value for the given VRK value.
         * Algorithm: if the given VRK's least significant byte is 0, then strip the 0; otherwise, subtract 1 from the
         * least significant byte, and fill the rest of the physical RK capacity with max unsigned bytes.
         *
         * <p>Examples: 1 2 0 0 => 1 2 0; 1 2 3 => 1 2 2 FF ... FF
         */
        private AttributeValue getMaxValueLessThan(byte[] vhk, byte[] vrk) {
            checkArgument(vrk.length > 0, "Comparison value may not be an empty binary array");
            if (vrk[vrk.length - 1] == 0) {
                // if the least significant byte is 0, then strip the 0
                if (vrk.length == 1) {
                    return keyMapper.toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY);
                } else {
                    byte[] newVrk = Arrays.copyOf(vrk, vrk.length - 1);
                    return keyMapper.toPhysicalRangeKey(virtualPk, vhk, newVrk);
                }
            } else {
                // subtract 1 from the least significant byte and fill the remainder of key capacity with max bytes
                byte[] newVrk = Arrays.copyOf(vrk, vrk.length);
                newVrk[vrk.length - 1] -= 1;
                return keyMapper.toPhysicalRangeKey(virtualPk, vhk, newVrk, true);
            }
        }

        /**
         * Returns the smallest VHK+VRK' value that is more than the VHK+VRK value for the given VRK value.
         * Algorithm: if the length of VHK+VRK is less than the maximum capacity, then return VHK+VRK+0; otherwise,
         * find the least signficant i such that VRK[i] is less than the max unsigned byte, add 1 to VRK[i], and return
         * VRK[0, i].
         *
         * <p>Examples: 1 2 3 => 1 2 3 0; 1 1 1 ... 1 4 5 FF FF => 1 1 1 ... 1 4 6
         */
        private AttributeValue getMinValueGreaterThan(byte[] vhk, byte[] vrk) {
            if (vhk.length + vrk.length < PhysicalRangeKeyBytesConverter.MAX_COMPOSITE_KEY_LENGTH) {
                byte[] newVrk = Arrays.copyOf(vrk, vrk.length + 1);
                newVrk[newVrk.length - 1] = 0;
                return keyMapper.toPhysicalRangeKey(virtualPk, vhk, newVrk);
            } else {
                for (int i = vrk.length - 1; i >= 0; i--) {
                    if (UnsignedBytes.compare(vrk[i], UnsignedBytes.MAX_VALUE) < 0) {
                        byte[] newVrk = i == vrk.length - 1 ? vrk : Arrays.copyOf(vrk, i + 1);
                        newVrk[i] += 1;
                        return keyMapper.toPhysicalRangeKey(virtualPk, vhk, newVrk);
                    }
                }
                throw new IllegalArgumentException(
                    "Cannot do range key comparison where exclusive lower bound is the maximum possible value");
            }
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
        private final Set<String> filterExpressionValuePlaceholders;

        // output
        private AttributeValue hkValue = null;
        private Optional<KeyFieldCondition> rkCondition = Optional.empty();

        KeyConditionExpressionVisitor(PrimaryKey primaryKey, Map<String, String> fieldPlaceholders,
                                      Map<String, AttributeValue> valuePlaceholders,
                                      Set<String> filterExpressionValuePlaceholders) {
            this.primaryKey = primaryKey;
            this.fieldPlaceholders = fieldPlaceholders;
            this.valuePlaceholders = valuePlaceholders;
            this.filterExpressionValuePlaceholders = filterExpressionValuePlaceholders;
        }

        @Override
        public Void visitKeyConditionPart(KeyConditionPartContext keyConditionPart) {
            IdContext id = keyConditionPart.id();
            if (id != null) {
                String fieldName = keyConditionPart.id().getText();
                if (fieldName.startsWith("#")) {
                    // if it's a field placeholder, remove it -- we'll generated a new one for translated expression.
                    // (we don't need to worry about the field placeholder also being used in the filter expression,
                    // since the filter expression can contain only non-key fields.)
                    fieldName = removeFieldPlaceholder(fieldName);
                }

                if (primaryKey.getHashKey().equals(fieldName)) {
                    checkArgument(hkValue == null, "Multiple hash key conditions");
                    ComparatorContext comparator = keyConditionPart.comparator();
                    checkArgument(comparator != null && comparator.getText().equals("="),
                        "Hash key condition must have operator '='");
                    String valuePlaceholder = keyConditionPart.literal(0).getText();
                    hkValue = getValueAndRemovePlaceholderIfNeeded(valuePlaceholder);
                } else if (primaryKey.getRangeKey().isPresent() && primaryKey.getRangeKey().get().equals(fieldName)) {
                    checkArgument(rkCondition.isEmpty(), "Multiple range key conditions");
                    ComparisonOperator op = getRangeKeyComparisonOp(keyConditionPart);
                    List<AttributeValue> values = keyConditionPart.literal().stream()
                        .map(LiteralContext::getText)
                        .map(this::getValueAndRemovePlaceholderIfNeeded)
                        .collect(Collectors.toList());
                    rkCondition = Optional.of(new KeyFieldCondition(op, values));
                } else {
                    throw new IllegalArgumentException("Key condition expression contains non-key field: " + fieldName);
                }
            }
            return visitChildren(keyConditionPart);
        }

        private String removeFieldPlaceholder(String placeholder) {
            String literal = null;
            if (fieldPlaceholders != null) {
                literal = fieldPlaceholders.remove(placeholder);
            }
            checkArgument(literal != null, "Referenced attribute name does not exist: %s", placeholder);
            return literal;
        }

        private AttributeValue getValueAndRemovePlaceholderIfNeeded(String placeholder) {
            AttributeValue value = null;
            if (valuePlaceholders != null) {
                // keep the value placeholder if it's also in the filter expression; otherwise remove it
                value = filterExpressionValuePlaceholders.contains(placeholder)
                    ? valuePlaceholders.get(placeholder)
                    : valuePlaceholders.remove(placeholder);
            }
            checkArgument(value != null, "Referenced attribute value does not exist: %s", placeholder);
            return value;
        }

        private ComparisonOperator getRangeKeyComparisonOp(KeyConditionPartContext keyConditionPart) {
            ComparatorContext singelValueComparator = keyConditionPart.comparator();
            if (singelValueComparator != null) {
                return getSingleRangeKeyValueComparisonOp(singelValueComparator.getText());
            } else {
                checkArgument(keyConditionPart.BETWEEN() != null,
                    "Invalid range key condition: %s", keyConditionPart.getText());
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
