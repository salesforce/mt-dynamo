/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.sample

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class SampleDataClassTest {

    @Test
    fun `just a little test to show that throwing kotlin in the mix is easy and awesome`() {
        val foo = "hello, from Kotlin!"
        val sample = SampleDataClass(foo)
        assertEquals(foo, sample.foo)
    }
}