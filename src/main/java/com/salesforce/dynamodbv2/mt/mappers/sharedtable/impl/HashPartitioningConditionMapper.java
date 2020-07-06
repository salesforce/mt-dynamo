/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.HashPartitioningKeyBytesConverter.toByteArray;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.toPhysicalRangeKey;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.HashPartitioningKeyBytesConverter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class HashPartitioningConditionMapper extends AbstractConditionMapper {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private final HashPartitioningKeyMapper keyMapper;
    private final Multimap<String, String> secondaryIndexFieldPartners;
    private final List<DynamoSecondaryIndex> secondaryIndexes;

    HashPartitioningConditionMapper(String context,
                                    DynamoTableDescription virtualTable,
                                    HashPartitioningItemMapper itemMapper,
                                    HashPartitioningKeyMapper keyMapper) {
        super(context, virtualTable, itemMapper);
        this.keyMapper = keyMapper;
        this.secondaryIndexFieldPartners = getSecondaryIndexFieldPartners(virtualTable);
        this.secondaryIndexes = virtualTable.getSis();
    }

    /**
     * Returns a Multimap of field pairings in composite secondary indexes, so we can easily enforce that a field can
     * be updated only if for each composite secondary index that the field is part of, the other index field is also
     * being updated.
     */
    private static Multimap<String, String> getSecondaryIndexFieldPartners(DynamoTableDescription virtualTable) {
        Multimap<String, String> fieldPartners = LinkedListMultimap.create();
        for (DynamoSecondaryIndex secondaryIndex : virtualTable.getSis()) {
            if (secondaryIndex.getPrimaryKey().getRangeKey().isPresent()) {
                String hashKey = secondaryIndex.getPrimaryKey().getHashKey();
                String rangeKey = secondaryIndex.getPrimaryKey().getRangeKey().get();
                fieldPartners.put(hashKey, rangeKey);
                fieldPartners.put(rangeKey, hashKey);
            }
        }
        return fieldPartners;
    }

    @Override
    protected void validateFieldsCanBeUpdated(UpdateActions allUpdateActions) {
        super.validateFieldsCanBeUpdated(allUpdateActions);

        Set<String> addUpdateFields = allUpdateActions.getAddActions().keySet();
        Set<String> setUpdateFields = allUpdateActions.getSetActions().keySet();

        addUpdateFields.forEach(updatedField -> {
            secondaryIndexes.stream().forEach(index -> {
                validateUpdatedFieldIsNotInIndexKey(updatedField, index);
            });
        });

        for (String field : setUpdateFields) {
            Collection<String> indexPartners = secondaryIndexFieldPartners.get(field);
            if (indexPartners != null) {
                for (String indexPartner : indexPartners) {
                    if (!(setUpdateFields).contains(indexPartner)) {
                        throw new IllegalArgumentException("Cannot update attribute " + field
                            + ", The other key attribute in a secondary index is not being updated");
                    }
                }
            }
        }
    }

    @Override
    public void applyToFilterExpression(RequestWrapper request) {
        // no-op, since we keep all original virtual fields
    }

    @Override
    protected String mapKeyConditionExpression(RequestWrapper request, RequestIndex requestIndex,
                                               AttributeValue virtualHkValue,
                                               Optional<AbstractConditionMapper.KeyFieldCondition> virtualRkCondition) {
        KeyConditionExpressionMapper expressionMapper = new KeyConditionExpressionMapper(requestIndex.getVirtualPk(),
            requestIndex.getPhysicalPk(), virtualHkValue, virtualRkCondition);
        return expressionMapper.mapKeyConditionExpression(request);
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
                return getRangeKeyEquals(request, toPhysicalRangeKey(virtualPk, virtualHkValue));
            }

            // "#vhk = :vhk", has virtual RK ==> "#prk BETWEEN vhk AND :vhk+FFFFFF"
            if (virtualRkCondition.isEmpty()) {
                byte[] vhk = toByteArray(virtualPk.getHashKeyType(), virtualHkValue);
                AttributeValue lower = toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY);
                AttributeValue upper = toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true);
                return getRangeKeyBetween(request, lower, upper);
            }

            ComparisonOperator comparisonOperator = virtualRkCondition.get().getOperator();
            List<AttributeValue> vrkValues = virtualRkCondition.get().getValues();

            // "#vhk = :vhk and #vrk = :vrk" ==> "#prk = :vhk+vrk“
            if (comparisonOperator == EQ) {
                AttributeValue value = toPhysicalRangeKey(virtualPk, virtualHkValue, vrkValues.get(0));
                return getRangeKeyEquals(request, value);
            }

            BetweenRange betweenRange = getBetweenRange(comparisonOperator, vrkValues);
            return getRangeKeyBetween(request, betweenRange.lower, betweenRange.upper);
        }

        private BetweenRange getBetweenRange(ComparisonOperator comparisonOperator, List<AttributeValue> vrkValues) {
            byte[] vhk = toByteArray(virtualPk.getHashKeyType(), virtualHkValue);
            ScalarAttributeType virtualRkType = virtualPk.getRangeKeyType().get();
            byte[] vrk = toByteArray(virtualRkType, vrkValues.get(0));
            switch (comparisonOperator) {
                case LT:
                    // "#vhk = :vhk and #vrk < :vrk" ==> "#prk BETWEEN :vhk AND :vhk+maxValueLessThan(vrk)“
                    // (for the upper bound, we compute the largest value that's smaller than the specified virtual RK
                    // RK value, since BETWEEN is inclusive)
                    return new BetweenRange(
                        toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY),
                        getMaxValueLessThan(vhk, vrk));
                case LE:
                    // "#vhk = :vhk and #vrk <= :vrk" ==> "#prk BETWEEN :vhk AND :vhk+vrk“
                    return new BetweenRange(
                        toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY),
                        toPhysicalRangeKey(virtualPk, vhk, vrk));
                case GT:
                    // #vhk = :vhk and #vrk > :vrk" ==> "#prk BETWEEN :vhk+minValueGreaterThan(vrk) AND :vhk+FFFFFF“
                    // (for the lower bound, we compute the smallest value that's greater than the specified virtual RK
                    // RK value, since BETWEEN is inclusive)
                    return new BetweenRange(
                        getMinValueGreaterThan(vhk, vrk),
                        toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true));
                case GE:
                    // "#vhk = :vhk and #vrk >= :vrk" ==> "#prk BETWEEN :vhk+vrk AND :vhk+FFFFFF“
                    return new BetweenRange(
                        toPhysicalRangeKey(virtualPk, vhk, vrk),
                        toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY, true));
                case BETWEEN:
                    // "#vhk = :vhk and #vrk BETWEEN :vrk1 AND :vrk2" ==> "#prk BETWEEN :vhk+vrk1 AND :vhk+vrk2“
                    byte[] vrkUpper = toByteArray(virtualRkType, vrkValues.get(1));
                    return new BetweenRange(
                        toPhysicalRangeKey(virtualPk, vhk, vrk),
                        toPhysicalRangeKey(virtualPk, vhk, vrkUpper));
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
                    return toPhysicalRangeKey(virtualPk, vhk, EMPTY_BYTE_ARRAY);
                } else {
                    byte[] newVrk = Arrays.copyOf(vrk, vrk.length - 1);
                    return toPhysicalRangeKey(virtualPk, vhk, newVrk);
                }
            } else {
                // subtract 1 from the least significant byte and fill the remainder of key capacity with max bytes
                byte[] newVrk = Arrays.copyOf(vrk, vrk.length);
                newVrk[vrk.length - 1] -= 1;
                return toPhysicalRangeKey(virtualPk, vhk, newVrk, true);
            }
        }

        /**
         * Returns the smallest VHK+VRK' value that is more than the VHK+VRK value for the given VRK value.
         * Algorithm: if the length of VHK+VRK is less than the maximum capacity, then return VHK+VRK+0; otherwise,
         * find the least significant i such that VRK[i] is less than the max unsigned byte, add 1 to VRK[i], and return
         * VRK[0, i].
         *
         * <p>Examples: 1 2 3 => 1 2 3 0; 1 1 1 ... 1 4 5 FF FF => 1 1 1 ... 1 4 6
         */
        private AttributeValue getMinValueGreaterThan(byte[] vhk, byte[] vrk) {
            if (vhk.length + vrk.length < HashPartitioningKeyBytesConverter.MAX_COMPOSITE_KEY_LENGTH) {
                byte[] newVrk = Arrays.copyOf(vrk, vrk.length + 1);
                newVrk[newVrk.length - 1] = 0;
                return toPhysicalRangeKey(virtualPk, vhk, newVrk);
            } else {
                for (int i = vrk.length - 1; i >= 0; i--) {
                    if (UnsignedBytes.compare(vrk[i], UnsignedBytes.MAX_VALUE) < 0) {
                        byte[] newVrk = i == vrk.length - 1 ? vrk : Arrays.copyOf(vrk, i + 1);
                        newVrk[i] += 1;
                        return toPhysicalRangeKey(virtualPk, vhk, newVrk);
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
            return String.format("%s BETWEEN %s AND %s", fieldPlaceholder, lowerPlaceholder, upperPlaceholder);
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

}
