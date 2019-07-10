package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.function.Predicate;

public class BinaryFieldPrefixFunction implements FieldPrefixFunction<ByteBuffer> {

    static final BinaryFieldPrefixFunction INSTANCE = new BinaryFieldPrefixFunction();

    private static final byte DELIMITER = 0x00;

    private BinaryFieldPrefixFunction() {
        super();
    }

    @Override
    public ByteBuffer apply(FieldValue<ByteBuffer> fieldValue) {
        final ByteBuffer value = fieldValue.getValue().asReadOnlyBuffer();
        final ByteBuffer buffer = newBuffer(fieldValue.getContext(), fieldValue.getTableName(), value.remaining());
        buffer.put(value);
        buffer.flip();
        return buffer;
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

    @Override
    public Predicate<ByteBuffer> createFilter(String context, String tableName) {
        // equivalent of String.startsWith
        final ByteBuffer prefix = newBuffer(context, tableName, 0).flip();
        final int end = prefix.limit();
        return b -> b.mismatch(prefix) == end;
    }

    private static int indexOf(byte[] array, byte target, int start) {
        for (int i = start; i < array.length; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }

    private static ByteBuffer newBuffer(String context, String tableName, int valueLength) {
        final byte[] contextBytes = context.getBytes(UTF_8);
        final byte[] tableNameBytes = tableName.getBytes(UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocate(contextBytes.length + tableNameBytes.length + valueLength + 2);
        buffer.put(contextBytes);
        buffer.put(DELIMITER);
        buffer.put(tableNameBytes);
        buffer.put(DELIMITER);
        return buffer;
    }

}
