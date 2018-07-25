/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
import org.junit.jupiter.api.Test;

/*
 * @author msgroi
 */
class FieldPrefixFunctionTest {

    private static final FieldPrefixFunction SUT = new FieldPrefixFunction(".");

    @Test
    void applyAndReverse() {
        FieldValue expected = new FieldValue("ctx", "table", "ctx.table.value", "value");

        FieldValue applied = SUT.apply(() -> "ctx", "table", "value");

        assertEquals(expected, applied);

        assertEquals(expected, SUT.reverse(applied.getQualifiedValue()));
    }

}