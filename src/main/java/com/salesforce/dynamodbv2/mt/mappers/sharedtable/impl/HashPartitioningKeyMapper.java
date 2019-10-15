/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

class HashPartitioningKeyMapper {

    @VisibleForTesting
    static final String DELIMITER = ".";

    private static final String DELIMITER_PATTERN = "\\.";

    static class HashPartitioningKeyPrefixFunction {

        static AttributeValue toPhysicalHashKey(String context, String virtualTableName, int bucket) {
            String stringValue = String.join(DELIMITER, context, virtualTableName, String.valueOf(bucket));
            return new AttributeValue().withB(ByteBuffer.wrap(stringValue.getBytes(StandardCharsets.UTF_8)));
        }

        static MtContextAndTable fromPhysicalHashKey(AttributeValue value) {
            String[] parts = fromPhysicalHashKeyToParts(value);
            return new MtContextAndTable(parts[0], parts[1]);
        }

        @VisibleForTesting
        static String[] fromPhysicalHashKeyToParts(AttributeValue value) {
            byte[] bytes = value.getB().array();
            String stringValue = new String(bytes, StandardCharsets.UTF_8);
            String[] parts = stringValue.split(DELIMITER_PATTERN);
            checkState(parts.length == 3, "Invalid hash key value: %s", stringValue);
            return parts;
        }
    }

    private final String virtualTableName;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final int numBucketsPerVirtualTable;

    HashPartitioningKeyMapper(String virtualTableName, MtAmazonDynamoDbContextProvider mtContext,
                              int numBucketsPerVirtualTable) {
        this.virtualTableName = virtualTableName;
        this.mtContext = mtContext;
        this.numBucketsPerVirtualTable = numBucketsPerVirtualTable;
    }

    AttributeValue toPhysicalHashKey(ScalarAttributeType virtualHkType, AttributeValue virtualHkValue) {
        int bucket = getBucketNumber(virtualHkType, virtualHkValue);
        return toPhysicalHashKey(bucket);
    }

    AttributeValue toPhysicalHashKey(int bucket) {
        return HashPartitioningKeyPrefixFunction.toPhysicalHashKey(mtContext.getContext(), virtualTableName, bucket);
    }

    int getBucketNumber(ScalarAttributeType virtualHkType, AttributeValue virtualHkValue) {
        return getPrimitiveValueHashCode(virtualHkType, virtualHkValue) % numBucketsPerVirtualTable;
    }

    int getNumberOfBucketsPerVirtualTable() {
        return numBucketsPerVirtualTable;
    }

    private int getPrimitiveValueHashCode(ScalarAttributeType type, AttributeValue value) {
        switch (type) {
            case S:
                return Objects.hashCode(value.getS());
            case N:
                return Objects.hashCode(new BigDecimal(value.getN()));
            case B:
                return Arrays.hashCode(value.getB().array());
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + type);
        }
    }

    AttributeValue toPhysicalRangeKey(PrimaryKey primaryKey, AttributeValue hk) {
        checkArgument(primaryKey.getRangeKey().isEmpty(), "Should not be serializing only hash key of composite key");
        return new AttributeValue().withB(PhysicalRangeKeyBytesConverter.toBytes(primaryKey.getHashKeyType(), hk));
    }

    AttributeValue toPhysicalRangeKey(PrimaryKey primaryKey, AttributeValue hk, AttributeValue rk) {
        checkArgument(primaryKey.getRangeKey().isPresent(), "Key should be composite");
        return new AttributeValue().withB(PhysicalRangeKeyBytesConverter.toBytes(primaryKey.getHashKeyType(), hk,
            primaryKey.getRangeKeyType().get(), rk));
    }

    AttributeValue toPhysicalRangeKey(PrimaryKey primaryKey, AttributeValue hk, byte[] rkBytes) {
        return toPhysicalRangeKey(primaryKey, hk, rkBytes, false);
    }

    AttributeValue toPhysicalRangeKey(PrimaryKey primaryKey, AttributeValue hk, byte[] rkBytes,
                                      boolean padWithMaxUnsignedByte) {
        checkArgument(primaryKey.getRangeKey().isPresent(), "Key should be composite");
        return new AttributeValue().withB(PhysicalRangeKeyBytesConverter.toBytes(primaryKey.getHashKeyType(), hk,
            rkBytes, padWithMaxUnsignedByte));
    }

    @VisibleForTesting
    static class PhysicalRangeKeyBytesConverter {

        // see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-partition-sort-keys
        private static final int MAX_KEY_LENGTH = 1024;

        static ByteBuffer toBytes(ScalarAttributeType hkType, AttributeValue hk, ScalarAttributeType rkType,
                                  AttributeValue rk) {
            byte[] rkb = toByteArray(rkType, rk);
            return toBytes(hkType, hk, rkb, false);
        }

        static ByteBuffer toBytes(ScalarAttributeType hkType, AttributeValue hk, byte[] rkb,
                                  boolean padWithMaxUnsignedByte) {
            byte[] hkb = toByteArray(hkType, hk);
            Preconditions.checkArgument(hkb.length + rkb.length <= MAX_KEY_LENGTH - 2);
            if (padWithMaxUnsignedByte) {
                ByteBuffer key = ByteBuffer.allocate(MAX_KEY_LENGTH);
                key.putShort((short) hkb.length).put(hkb).put(rkb);
                while (key.hasRemaining()) {
                    key.put(UnsignedBytes.MAX_VALUE);
                }
                return key.flip();
            } else {
                ByteBuffer key = ByteBuffer.allocate(2 + hkb.length + rkb.length);
                return key.putShort((short) hkb.length).put(hkb).put(rkb).flip();
            }
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
     * Encodes BigDecimals into byte arrays such that when sorted as <b>unsigned</b> bytes, the numerical ordering is
     * preserved.
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
     * <li>SIGNUM: 0, 1, or 2, corresponding to negative, zero, or positive (1 byte)
     * <li>EXPONENT: Represents exponent when number is in normalized notation, where the range is [0, 255], mapped to
     * the Dynamo range [-130, 125] (1 byte)
     * <li>D_1 to D_n: The n (at most 38) significand digits, from most significant to least (each digit takes 1 byte)
     * <li>When the sign is negative, the exponent and significand digits' bits are flipped
     * </ul></p>
     */
    @VisibleForTesting
    static class BigDecimalSortedBytesConverter {

        private static final int MIN_EXPONENT = -130;
        private static final int MAX_EXPONENT = 125;

        static byte[] encode(BigDecimal bigDecimal) {
            bigDecimal = bigDecimal.stripTrailingZeros();
            int precision = bigDecimal.precision();
            byte[] byteArray = new byte[2 + precision];

            // first byte represents the signum
            byteArray[0] = toSignumByte(bigDecimal.signum());

            // second byte represents the exponent, when in the number is in normalized notation
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
                // write it in the least significant byte not yet written
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

        private static byte toSignumByte(int signum) {
            // map signum from range [-1, 1] to non-negative range [0, 2]
            return (byte)(signum + 1);
        }

        private static int fromSignumByte(byte signumByte) {
            return signumByte - 1;
        }

        private static byte toExponentByte(int exponent) {
            // map [-130, 125] to [0, 255]
            return UnsignedBytes.checkedCast(exponent - MIN_EXPONENT);
        }

        private static int fromExponentByte(byte exponentByte) {
            return UnsignedBytes.toInt(exponentByte) + MIN_EXPONENT;
        }

        static BigDecimal decode(byte[] byteArray) {
            // get signum from first byte
            int signum = fromSignumByte(byteArray[0]);

            // get exponent from second byte, and the number of significand digits from the length of the remaining
            // bytes, so we can recover the scale
            int normalizedExponent = fromExponentByte(flipByteIfNegative(byteArray[1], signum));
            int precision = byteArray.length - 2;
            int scale = precision - normalizedExponent - 1;

            // compute the significand, working backwards from the least significant digit
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
