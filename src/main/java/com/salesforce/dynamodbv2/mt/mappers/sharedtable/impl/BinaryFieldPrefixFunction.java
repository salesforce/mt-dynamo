package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

public class BinaryFieldPrefixFunction implements FieldPrefixFunction<ByteBuffer> {

    private static final byte DELIMITER = 0x00;

    @Override
    public ByteBuffer apply(FieldValue<ByteBuffer> fieldValue) {
        final byte[] context = fieldValue.getContext().getBytes(UTF_8);
        final byte[] tableName = fieldValue.getTableName().getBytes(UTF_8);
        final ByteBuffer value = fieldValue.getValue();
        ByteBuffer qualifiedValue = ByteBuffer.allocate(context.length + tableName.length + value.remaining() + 2);
        qualifiedValue.put(context);
        qualifiedValue.put(DELIMITER);
        qualifiedValue.put(tableName);
        qualifiedValue.put(DELIMITER);
        qualifiedValue.put(value);
        value.flip();
        qualifiedValue.flip();
        return qualifiedValue;
    }

    @Override
    public FieldValue<ByteBuffer> reverse(ByteBuffer b) {
        byte[] qualifiedValue = b.array();
        int idx = indexOf(qualifiedValue, DELIMITER, 0);
        checkArgument(idx != -1);
        final String context = new String(qualifiedValue, 0, idx, UTF_8);

        idx++;
        int idx2 = indexOf(qualifiedValue, DELIMITER, idx);
        checkArgument(idx2 != -1);
        final String tableName = new String(qualifiedValue, idx, idx2 - idx, UTF_8);

        idx2++;
        byte[] value = new byte[qualifiedValue.length - idx2];
        System.arraycopy(qualifiedValue, idx2, value, 0, value.length);

        return new FieldValue<>(context, tableName, ByteBuffer.wrap(value));
    }

    private static int indexOf(byte[] array, byte target, int start) {
        for (int i = start; i < array.length; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
