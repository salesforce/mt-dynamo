/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests FieldPrefixFunction.
 *
 * @author msgroi
 */
class FieldPrefixFunctionTest {

    private static final StringFieldPrefixFunction SUT_S = new StringFieldPrefixFunction();
    private static final BinaryFieldPrefixFunction SUT_B = new BinaryFieldPrefixFunction();

    private static Object[] binaryData(String context, String table, String value) {
        return new Object[] {
            SUT_B, context, table, UTF_8.encode(value), ByteBuffer
            .allocate(context.length() + table.length() + value.length() + 2)
            .put(UTF_8.encode(context)).put((byte) 0x00)
            .put(UTF_8.encode(table)).put((byte) 0x00)
            .put(UTF_8.encode(value))
            .flip()
        };
    }

    static Stream<Object[]> data() {
        return Arrays.stream(new Object[][] {
            { SUT_S, "ctx", "table", "value", "ctx/table/value" },
            { SUT_S, "ctx2", "com.salesforce.zero.someObject", "value2", "ctx2/com.salesforce.zero.someObject/value2" },
            { SUT_S, "ctx_3", "My-Object", "prefix/suffix", "ctx_3/My-Object/prefix/suffix" },
            binaryData("ctx", "table", "value"),
            binaryData("ctx", "table", "val\u0000ue"),
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    <V> void applyAndReverse(FieldPrefixFunction<V> sut, String context, String tableIndex, V unqualifiedValue,
                             V qualifiedValue) {
        FieldValue<V> expected = new FieldValue<>(context, tableIndex, unqualifiedValue);

        V actual = sut.apply(expected);

        assertEquals(qualifiedValue, actual);

        assertEquals(expected, sut.reverse(actual));
    }

}