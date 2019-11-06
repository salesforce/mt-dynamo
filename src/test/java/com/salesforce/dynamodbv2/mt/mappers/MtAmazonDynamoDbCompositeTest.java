/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MtAmazonDynamoDbCompositeTest {

    @ParameterizedTest
    @MethodSource("dataForValidateTest")
    void testValidate(Collection<MtAmazonDynamoDbBase> delegates, String error) {
        if (error != null) {
            try {
                new MtAmazonDynamoDbComposite(delegates, null, null);
                fail("Should have thrown IllegalArgumentException with message: " + error);
            } catch (IllegalArgumentException e) {
                assertEquals(error, e.getMessage());
            }
        } else {
            new MtAmazonDynamoDbComposite(delegates, null, null);
        }
    }

    private static Stream<Arguments> dataForValidateTest() {
        MtAmazonDynamoDbContextProvider mtContext = mock(MtAmazonDynamoDbContextProvider.class);
        AmazonDynamoDB amazonDynamoDb = mock(AmazonDynamoDB.class);
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        String scanTenantKey = "testScanTenantKey";
        String scanVirtualTableKey = "testScanVirtualTableKey";

        MtAmazonDynamoDbBase first = new MtAmazonDynamoDbBase(mtContext, amazonDynamoDb, meterRegistry, scanTenantKey,
            scanVirtualTableKey);
        MtAmazonDynamoDbBase second = new MtAmazonDynamoDbBase(mtContext, amazonDynamoDb, meterRegistry, scanTenantKey,
            scanVirtualTableKey);

        return Stream.of(
            // valid
            Arguments.of(ImmutableList.of(first, second), null),
            // invalid
            Arguments.of(null, "Must provide at least one delegate"),
            Arguments.of(Collections.emptyList(), "Must provide at least one delegate"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(null, amazonDynamoDb, meterRegistry, scanTenantKey, scanVirtualTableKey)),
                "Delegates must share the same mt context provider"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(mtContext, null, meterRegistry, scanTenantKey, scanVirtualTableKey)),
                "Delegates must share the same parent AmazonDynamoDB"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(mtContext, amazonDynamoDb, null, scanTenantKey, scanVirtualTableKey)),
                "Delegates must share the same meter registry"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(mtContext, amazonDynamoDb, meterRegistry, null, scanVirtualTableKey)),
                "Delegates must share the same scan tenant key"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(mtContext, amazonDynamoDb, meterRegistry, scanTenantKey, null)),
                "Delegates must share the same scan virtual table key")
        );
    }
}
