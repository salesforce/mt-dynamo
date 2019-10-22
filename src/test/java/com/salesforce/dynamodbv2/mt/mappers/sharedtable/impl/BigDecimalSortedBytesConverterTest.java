/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.BigDecimalSortedBytesConverter.decode;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.BigDecimalSortedBytesConverter.encode;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BigDecimalSortedBytesConverterTest {

    @Test
    void testEncodeDecode() {
        validateEncodeDecode(new BigDecimal("12.34567"));
        validateEncodeDecode(new BigDecimal("-987.654"));
        validateEncodeDecode(new BigDecimal("1000"));
        validateEncodeDecode(new BigDecimal("-0.00987"));
        validateEncodeDecode(new BigDecimal("-1.0000000000000000000000000000000000000E-130"));
        validateEncodeDecode(new BigDecimal("9.9999999999999999999999999999999999999E+125"));
        validateEncodeDecode(new BigDecimal("1.0000000000000000000000000000000000000E-130"));
        validateEncodeDecode(new BigDecimal("-9.9999999999999999999999999999999999999E+125"));
    }

    private void validateEncodeDecode(BigDecimal bigDecimal) {
        byte[] bytes = encode(bigDecimal);
        assertEquals(bigDecimal.stripTrailingZeros(), decode(bytes));
    }

    @Test
    void testNumericalOrderPreserved() {
        List<BigDecimal> numbers = Lists.newArrayList(
            new BigDecimal("12.34567"),
            new BigDecimal("-987.654"),
            new BigDecimal("1000"),
            new BigDecimal("-0.00987"),
            new BigDecimal("-1.0000000000000000000000000000000000000E-130"),
            new BigDecimal("9.9999999999999999999999999999999999999E+125"),
            new BigDecimal("1.0000000000000000000000000000000000000E-130"),
            new BigDecimal("-9.9999999999999999999999999999999999999E+125")
        );
        List<byte[]> byteArrays = numbers.stream()
            .map(HashPartitioningKeyMapper.BigDecimalSortedBytesConverter::encode)
            .collect(Collectors.toList());

        Collections.sort(numbers);
        numbers = numbers.stream().map(BigDecimal::stripTrailingZeros).collect(Collectors.toList());

        byteArrays.sort(Arrays::compareUnsigned);
        List<BigDecimal> decodedNumbers = byteArrays.stream()
            .map(HashPartitioningKeyMapper.BigDecimalSortedBytesConverter::decode)
            .collect(Collectors.toList());
        assertEquals(numbers, decodedNumbers);
    }
}
