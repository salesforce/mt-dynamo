/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * Maps virtual items to physical items and vice versa for shared tables using hash partitioning. More specifically,
 * <pre>
 *     hashKey = tenantId + virtualTableName + hash(virtualHashKey) % numPartitions
 *     rangeKey = virtualHashKey + virtualRangeKey
 * </pre>
 */
class HashPartitioningItemMapper implements ItemMapper {

    private static final int MAX_KEY_LENGTH = 1024;

    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;
    private final UnaryOperator<DynamoSecondaryIndex> lookUpPhysicalSecondaryIndex;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final int numBucketsPerVirtualHashKey;
    private final char delimiter;

    HashPartitioningItemMapper(DynamoTableDescription virtualTable,
                               DynamoTableDescription physicalTable,
                               UnaryOperator<DynamoSecondaryIndex> lookUpPhysicalSecondaryIndex,
                               MtAmazonDynamoDbContextProvider mtContext,
                               int numBucketsPerVirtualHashKey,
                               char delimiter) {
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.lookUpPhysicalSecondaryIndex = lookUpPhysicalSecondaryIndex;
        this.mtContext = mtContext;
        this.numBucketsPerVirtualHashKey = numBucketsPerVirtualHashKey;
        this.delimiter = delimiter;
    }

    @Override
    public Map<String, AttributeValue> applyForWrite(Map<String, AttributeValue> virtualItem) {
        virtualItem = Collections.unmodifiableMap(virtualItem);
        Map<String, AttributeValue> physicalItem = new HashMap<>(virtualItem);

        // move table primary key fields
        addPhysicalKeys(virtualItem, physicalItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // copy index fields
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            addPhysicalKeys(virtualItem, physicalItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), false);
        }

        return physicalItem;
    }

    @Override
    public Map<String, AttributeValue> applyToKeyAttributes(Map<String, AttributeValue> virtualItem,
                                                            @Nullable DynamoSecondaryIndex virtualSi) {
        virtualItem = Collections.unmodifiableMap(virtualItem);
        Map<String, AttributeValue> physicalItem = new HashMap<>();

        // extract table primary key fields
        addPhysicalKeys(virtualItem, physicalItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // extract secondary index fields if a secondary index is specified
        if (virtualSi != null) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            addPhysicalKeys(virtualItem, physicalItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), true);
        }

        return physicalItem;
    }

    private void addPhysicalKeys(Map<String, AttributeValue> virtualItem, Map<String, AttributeValue> physicalItem,
                                 PrimaryKey virtualPK, PrimaryKey physicalPK, boolean required) {
        AttributeValue virtualHkValue = virtualItem.get(virtualPK.getHashKey());
        if (required) {
            checkNotNull(virtualHkValue, "Item is missing hash key '" + virtualPK.getHashKey() + "'");
        }
        if (virtualPK.getRangeKey().isPresent()) {
            AttributeValue virtualRkValue = virtualItem.get(virtualPK.getRangeKey().get());
            if (required) {
                checkNotNull(virtualRkValue, "Item is missing range key '" + virtualPK.getRangeKey().get() + "'");
            }
            if (virtualHkValue != null && virtualRkValue != null) {
                // add physical HK
                putPhysicalHashKey(physicalItem, virtualPK.getHashKeyType(), virtualHkValue, physicalPK);

                // add physical RK
                ByteBuffer physicalRangeKeyValue = AttributeBytesConverter.toBytes(
                    virtualPK.getHashKeyType(), virtualHkValue, virtualPK.getRangeKeyType().get(), virtualRkValue);
                putBytesInField(physicalItem, physicalPK.getRangeKey().get(), physicalRangeKeyValue);

                // remove virtual HK and RK if needed
                physicalItem.remove(virtualPK.getHashKey());
                physicalItem.remove(virtualPK.getRangeKey().get());
            }
        } else {
            if (virtualHkValue != null) {
                // add physical HK
                putPhysicalHashKey(physicalItem, virtualPK.getHashKeyType(), virtualHkValue, physicalPK);

                // add physical RK
                ByteBuffer physicalRangeKeyValue = AttributeBytesConverter.toBytes(virtualPK.getHashKeyType(),
                    virtualHkValue);
                putBytesInField(physicalItem, physicalPK.getRangeKey().get(), physicalRangeKeyValue);

                // remove virtual HK if needed
                physicalItem.remove(virtualPK.getHashKey());
            }
        }
    }

    private void putBytesInField(Map<String, AttributeValue> item, String field, ByteBuffer bytes) {
        item.put(field, new AttributeValue().withB(bytes));
    }

    private void putPhysicalHashKey(Map<String, AttributeValue> item, ScalarAttributeType virtualHkType,
                                    AttributeValue virtualHkValue, PrimaryKey physicalPK) {
        Object primitiveValue = getPrimitiveValue(virtualHkType, virtualHkValue);
        checkNotNull(primitiveValue, "Hash key value cannot be null");
        int salt = primitiveValue.hashCode() % numBucketsPerVirtualHashKey;

        // TODO we don't need to include virtual table name if physical table is for one mt table only
        String value = mtContext.getContext() + delimiter + virtualTable.getTableName() + delimiter + salt;

        putBytesInField(item, physicalPK.getHashKey(), ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
    }

    private Object getPrimitiveValue(ScalarAttributeType type, AttributeValue value) {
        switch (type) {
            case S:
                return value.getS();
            case N:
                return value.getN();
            case B:
                return value.getB().array();
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + type);
        }
    }

    @Override
    public Map<String, AttributeValue> reverse(Map<String, AttributeValue> physicalItem) {
        if (physicalItem == null) {
            return null;
        }

        Map<String, AttributeValue> virtualItem = new HashMap<>(physicalItem);

        // move table primary key fields
        removePhysicalKeys(virtualItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // copy index fields
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            removePhysicalKeys(virtualItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), false);
        }

        return virtualItem;
    }

    private void removePhysicalKeys(Map<String, AttributeValue> item, PrimaryKey virtualPK,
                                    PrimaryKey physicalPK, boolean required) {
        item.remove(physicalPK.getHashKey());
        AttributeValue physicalRkValue = item.remove(physicalPK.getRangeKey().get());
        if (required) {
            checkNotNull(physicalRkValue, "Physical item missing RK value");
        }

        if (physicalRkValue != null) {
            if (virtualPK.getRangeKey().isPresent()) {
                AttributeValue[] vs = AttributeBytesConverter.fromBytes(virtualPK.getHashKeyType(),
                    virtualPK.getRangeKeyType().get(), physicalRkValue.getB());
                item.put(virtualPK.getHashKey(), vs[0]);
                item.put(virtualPK.getRangeKey().get(), vs[1]);
            } else {
                item.put(virtualPK.getHashKey(), AttributeBytesConverter.fromBytes(virtualPK.getHashKeyType(),
                    physicalRkValue.getB()));
            }
        }
    }

    @VisibleForTesting
    static class AttributeBytesConverter {

        static ByteBuffer toBytes(ScalarAttributeType hkType, AttributeValue hk, ScalarAttributeType rkType,
                                  AttributeValue rk) {
            byte[] hkb = toByteArray(hkType, hk);
            byte[] rkb = toByteArray(rkType, rk);
            Preconditions.checkArgument(hkb.length + rkb.length <= MAX_KEY_LENGTH - 2);
            ByteBuffer key = ByteBuffer.allocate(2 + hkb.length + rkb.length);
            return key.putShort((short) hkb.length).put(hkb).put(rkb).flip();
        }

        static ByteBuffer toBytes(ScalarAttributeType type, AttributeValue value) {
            return ByteBuffer.wrap(toByteArray(type, value));
        }

        private static byte[] toByteArray(ScalarAttributeType type, AttributeValue value) {
            switch (type) {
                case S:
                    return value.getS().getBytes(StandardCharsets.UTF_8);
                case N:
                    BigDecimal bigDecimal = new BigDecimal(value.getN());
                    return BigDecimalSortedBytesConverter.encode(bigDecimal);
                case B:
                    return value.getB().array();
                default:
                    throw new UnsupportedOperationException("Unsupported field type: " + type);
            }
        }

        static AttributeValue[] fromBytes(ScalarAttributeType hkType, ScalarAttributeType rkType, ByteBuffer buf) {
            short hkValueLength = buf.getShort();
            AttributeValue hkValue = fromBytes(hkType, buf, hkValueLength);
            AttributeValue rkValue = fromBytes(rkType, buf, buf.remaining());
            return new AttributeValue[] { hkValue, rkValue };
        }

        static AttributeValue fromBytes(ScalarAttributeType type, ByteBuffer buf) {
            return fromBytes(type, buf, buf.limit());
        }

        private static AttributeValue fromBytes(ScalarAttributeType type, ByteBuffer buf, int size) {
            byte[] bytes = new byte[size];
            buf.get(bytes);
            switch (type) {
                case S:
                    return new AttributeValue(new String(bytes, StandardCharsets.UTF_8));
                case N:
                    return new AttributeValue().withN(BigDecimalSortedBytesConverter.decode(bytes).toPlainString());
                case B:
                    return new AttributeValue().withB(ByteBuffer.wrap(bytes));
                default:
                    throw new UnsupportedOperationException("Unsupported field type: " + type);
            }
        }
    }

    /**
     * Encodes BigDecimals into byte arrays such that the numerical ordering is preserved.
     * <p/>
     * In Dynamo, a number can have up to 38 digits of precision, and can be positive, negative, or zero.
     * <p><ul>
     * <li>Positive range: 1E-130 to 9.9999999999999999999999999999999999999E+125
     * <li>Negative range: -9.9999999999999999999999999999999999999E+125 to -1E-130
     * </ul></p>
     * (see doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-data-types)
     * <p/>
     * The format we use is SIGNUM + EXPONENT + D_1 + ... + D_n, where
     * <p><ul>
     * <li>SIGNUM: -1, 0, or 1, corresponding to negative, zero, or positive (1 byte)
     * <li>EXPONENT: Represents exponent when number is in normalized notation (range is [-128, 127], mapped to the
     * Dynamo range [-130, 125]; 1 byte)
     * <li>D_1 to D_n: The n (at most 38) significand digits, from most significant to least (each digit takes 1 byte)
     * <li>When the sign is negative, the exponent and significand digits' bits are flipped
     * </ul></p>
     */
    @VisibleForTesting
    static class BigDecimalSortedBytesConverter {

        private static final int MIN_EXPONENT = -130;
        private static final int MAX_EXPONENT = 125;
        private static final int MIN_BYTE_EXPONENT_DIFF = (int) Byte.MIN_VALUE - MIN_EXPONENT;

        static byte[] encode(BigDecimal bigDecimal) {
            bigDecimal = bigDecimal.stripTrailingZeros();
            int precision = bigDecimal.precision();
            byte[] byteArray = new byte[2 + precision];

            // first byte is the signum
            byteArray[0] = (byte) bigDecimal.signum();

            // second byte is the exponent, when in normalized notation
            // e.g., 12.345 has precision 5 and scale 3, and in normalized notation is 1.2345 x 10^1 -> exponent = 1
            int normalizedExponent = precision - bigDecimal.scale() - 1;
            checkState(normalizedExponent >= MIN_EXPONENT);
            checkState(normalizedExponent <= MAX_EXPONENT);
            byteArray[1] = toExponentByte(normalizedExponent);

            // the rest of the bytes are the significand. work backwards, from the least significant digit to the most.
            BigInteger significand = bigDecimal.unscaledValue();
            for (int i = byteArray.length - 1; i >= 2; i--) {
                BigInteger[] quotientAndRemainder = significand.divideAndRemainder(BigInteger.TEN);
                // remove least significant digit
                significand = quotientAndRemainder[0];
                // write it in the least signficant byte not yet written
                byteArray[i] = quotientAndRemainder[1].byteValueExact();
            }

            // if the number is negative, flip the exponent and significand bytes
            if (bigDecimal.signum() < 0) {
                for (int i = 1; i < byteArray.length; i++) {
                    byteArray[i] = (byte) ~byteArray[i];
                }
            }

            return byteArray;
        }

        private static byte toExponentByte(int exponent) {
            return (byte) (exponent + MIN_BYTE_EXPONENT_DIFF);
        }

        private static int fromExponentByte(byte exponentByte) {
            return (int) exponentByte - MIN_BYTE_EXPONENT_DIFF;
        }

        static BigDecimal decode(byte[] byteArray) {
            // get signum from first byte
            int signum = byteArray[0];

            // get exponent from second byte, and the number of signficand digits from the length of the remaining
            // bytes, so we can recover the scale
            int normalizedExponent = fromExponentByte(flipByteIfNegative(byteArray[1], signum));
            int precision = byteArray.length - 2;
            int scale = precision - normalizedExponent - 1;

            // compute the significand, working backwards from the least signficant digit
            BigInteger significand = BigInteger.ZERO;
            BigInteger power = BigInteger.ONE;
            for (int i = byteArray.length - 1; i >= 2; i--) {
                byte digit = flipByteIfNegative(byteArray[i], signum);
                significand = significand.add(BigInteger.valueOf(digit).multiply(power));
                power = power.multiply(BigInteger.TEN);
            }
            return new BigDecimal(significand, scale);
        }

        private static byte flipByteIfNegative(byte original, int signum) {
            return signum < 0 ? (byte) ~original : original;
        }
    }
}
