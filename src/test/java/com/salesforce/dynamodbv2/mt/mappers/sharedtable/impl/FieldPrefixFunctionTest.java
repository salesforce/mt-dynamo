/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;

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

    private static final FieldPrefixFunction SUT = new FieldPrefixFunction('.');

    static Stream<Object[]> data() {
        return Arrays.stream(new Object[][] {
            { "ctx", "table", "value", "ctx.table.value" },
            { "ctx2", "com.salesforce.zero.someObject", "value2", "ctx2.com\\.salesforce\\.zero\\.someObject.value2" }
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void applyAndReverse(String context, String tableIndex, String unqualifiedValue, String qualifiedValue) {
        FieldValue expected = new FieldValue(context, tableIndex, qualifiedValue, unqualifiedValue);

        FieldValue applied = SUT.apply(new MtAmazonDynamoDbContextProvider() {
            @Override
            public String getContext() {
                return context;
            }

            @Override
            public void withContext(String org, Runnable runnable) {

            }
        }, tableIndex, unqualifiedValue);

        assertEquals(expected, applied);

        assertEquals(expected, SUT.reverse(applied.getQualifiedValue()));
    }

}