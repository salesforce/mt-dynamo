package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Utility for parsing a FieldValue from from a binary hash key value (useful for debugging purposes).
 */
class BinaryHashKeyDecoder {

    /**
     * Prints context, table, and value stored in a binary hash key value. The hash key value must be supplied as the
     * first argument as a Base64 encoded string and the value is printed as a Base64 encoded string.
     *
     * @param args Base64 encoded hash key value.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Must specify Base64 encoded hash key value as first argument.");
            System.exit(-1);
        }
        byte[] value = null;
        try {
            value = Base64.getDecoder().decode(args[0]);
        } catch (IllegalArgumentException e) {
            System.err.println("First argument is not valid Base64 encoded string");
            System.exit(-1);
        }
        FieldValue<ByteBuffer> fieldValue = BinaryFieldPrefixFunction.INSTANCE.reverse(ByteBuffer.wrap(value));
        System.out.println("========================================");
        System.out.println("Context: " + fieldValue.getContext());
        System.out.println("TableName: " + fieldValue.getTableName());
        System.out.println("Value: " + Base64.getEncoder().encodeToString(fieldValue.getValue().array()));
        System.out.println("========================================");
    }

}
