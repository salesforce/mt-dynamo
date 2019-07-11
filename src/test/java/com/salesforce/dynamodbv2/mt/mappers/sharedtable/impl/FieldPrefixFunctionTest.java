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

    private static String qualifiedStringValue(String context, String table, String value) {
        return context + '/' + table + '/' + value;
    }

    private static ByteBuffer qualifiedBinaryValue(String context, String table, String value) {
        return ByteBuffer
            .allocate(context.length() + table.length() + value.length() + 2)
            .put(UTF_8.encode(context)).put((byte) 0x00)
            .put(UTF_8.encode(table)).put((byte) 0x00)
            .put(UTF_8.encode(value))
            .flip();
    }

    private static Stream<Object[]> forEach(Stream<Object[]> stream) {
        return stream.flatMap(e -> Stream.of(
            new Object[] { StringFieldPrefixFunction.INSTANCE, e[0], e[1], e[2],
                qualifiedStringValue(e[0].toString(), e[1].toString(), e[2].toString()) },
            new Object[] { BinaryFieldPrefixFunction.INSTANCE, e[0], e[1], UTF_8.encode(e[2].toString()),
                qualifiedBinaryValue(e[0].toString(), e[1].toString(), e[2].toString()) }
        ));
    }

    static Stream<Object[]> data() {
        return forEach(Arrays.stream(new Object[][] {
            { "ctx", "table", "value" },
            { "ctx2", "com.salesforce.zero.someObject", "value2" },
            { "ctx_3", "My-Object", "prefix/suffix" },
            { "ctx", "table", "val\u0000ue" }
        }));
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

    private static Stream<Object[]> filterForEach(Stream<Object[]> stream) {
        return stream.flatMap(e -> Stream.of(
            new Object[] { StringFieldPrefixFunction.INSTANCE, e[0], e[1],
                qualifiedStringValue(e[2].toString(), e[3].toString(), e[4].toString()), e[5] },
            new Object[] { BinaryFieldPrefixFunction.INSTANCE, e[0], e[1],
                qualifiedBinaryValue(e[2].toString(), e[3].toString(), e[4].toString()), e[5] }
        ));
    }

    static Stream<Object[]> filterData() {
        return filterForEach(Arrays.stream(new Object[][] {
            { "ctx", "table", "ctx", "table", "0", true },
            { "ctx", "table", "ctx", "table", "abc", true },
            { "ctx", "table", "ct", "table0", "0", false },
            { "ctx", "table", "ctx", "table1", "0", false },
            { "ctx", "table", "c", "t", "x", false },
            { "ctx", "table", "ctx2", "table2", "abc", false }
        }));
    }

    @ParameterizedTest
    @MethodSource("filterData")
    <V> void filter(FieldPrefixFunction<V> sut, String context, String tableName, V qualifiedValue,
                    boolean expected) {
        assertEquals(expected, sut.createFilter(context, tableName).test(qualifiedValue));
    }
}