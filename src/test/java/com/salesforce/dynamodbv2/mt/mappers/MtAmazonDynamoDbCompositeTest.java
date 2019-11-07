/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MtAmazonDynamoDbCompositeTest {

    private static final AtomicReference<String> CONTEXT = new AtomicReference<>(null);
    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT_PROVIDER = () -> Optional.ofNullable(CONTEXT.get());
    private static final String SCAN_TENANT_KEY = "testScanTenantKey";
    private static final String SCAN_VIRTUAL_TABLE_KEY = "testScanVirtualTableKey";

    private AmazonDynamoDB amazonDynamoDb;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void beforeEach() {
        amazonDynamoDb = mock(AmazonDynamoDB.class);
        meterRegistry = mock(MeterRegistry.class);
    }

    @Test
    void testGetDelegate() {
        MtAmazonDynamoDbBase first = getMockMtAmazonDynamoDb("table1");
        MtAmazonDynamoDbBase second = getMockMtAmazonDynamoDb("table2");
        MtAmazonDynamoDbComposite composite = new MtAmazonDynamoDbComposite(ImmutableList.of(first, second),
            () -> CONTEXT.get().equals("1") ? first : second,
            physicalTableName -> physicalTableName.equals("table1") ? first : second);

        CONTEXT.set("1");
        assertSame(first, composite.getDelegateFromContext());
        CONTEXT.set("2");
        assertSame(second, composite.getDelegateFromContext());

        assertSame(first, composite.getDelegateFromPhysicalTableName("table1"));
        assertSame(second, composite.getDelegateFromPhysicalTableName("table2"));
    }

    @Test
    void testIsMtTable() {
        MtAmazonDynamoDbBase first = getMockMtAmazonDynamoDb("table1");
        MtAmazonDynamoDbBase second = getMockMtAmazonDynamoDb("table2");
        MtAmazonDynamoDbComposite composite = new MtAmazonDynamoDbComposite(ImmutableList.of(first, second),
            null, null);

        assertTrue(composite.isMtTable("table1"));
        assertTrue(composite.isMtTable("table2"));
        assertFalse(composite.isMtTable("table3"));
    }

    @Test
    void testScan() {
        MtAmazonDynamoDbBase first = getMockMtAmazonDynamoDb("table1");
        MtAmazonDynamoDbBase second = getMockMtAmazonDynamoDb("table2");
        MtAmazonDynamoDbComposite composite = new MtAmazonDynamoDbComposite(ImmutableList.of(first, second),
            () -> CONTEXT.get().equals("1") ? first : second,
            physicalTableName -> physicalTableName.equals("table1") ? first : second);

        CONTEXT.set("1");
        ScanRequest scanRequest = new ScanRequest();
        composite.scan(scanRequest);
        verify(first).scan(scanRequest);

        CONTEXT.set("2");
        composite.scan(scanRequest);
        verify(second).scan(scanRequest);

        CONTEXT.set(null);
        scanRequest.withTableName("table1");
        composite.scan(scanRequest);
        verify(first, times(2)).scan(scanRequest);
    }

    @Test
    void testListTables() {
        MtAmazonDynamoDbBase first = getMockMtAmazonDynamoDb("table1");
        MtAmazonDynamoDbBase second = getMockMtAmazonDynamoDb("table2");
        MtAmazonDynamoDbComposite composite = new MtAmazonDynamoDbComposite(ImmutableList.of(first, second),
            () -> CONTEXT.get().equals("1") ? first : second,
            physicalTableName -> physicalTableName.equals("table1") ? first : second);

        CONTEXT.set("1");
        composite.listTables();
        verify(first).listTables(any(), any());

        CONTEXT.set("2");
        composite.listTables();
        verify(second).listTables(any(), any());

        CONTEXT.set(null);
        when(amazonDynamoDb.listTables(any(), any())).thenReturn(
            new ListTablesResult().withTableNames("table1", "table2", "table3"));
        assertEquals(ImmutableList.of("table1", "table2"), composite.listTables().getTableNames());
    }

    @Test
    void testStreams() {
        MtAmazonDynamoDbBase first = getMockMtAmazonDynamoDb("table1");
        MtAmazonDynamoDbBase second = getMockMtAmazonDynamoDb("table2");
        MtAmazonDynamoDbComposite composite = new MtAmazonDynamoDbComposite(ImmutableList.of(first, second),
            () -> CONTEXT.get().equals("1") ? first : second,
            physicalTableName -> physicalTableName.equals("table1") ? first : second);

        MtAmazonDynamoDbStreamsBase<? extends MtAmazonDynamoDbBase> firstStreams
            = (MtAmazonDynamoDbStreamsBase<? extends MtAmazonDynamoDbBase>) mock(MtAmazonDynamoDbStreamsBase.class);
        MtAmazonDynamoDbStreamsBase<? extends MtAmazonDynamoDbBase> secondStreams
            = (MtAmazonDynamoDbStreamsBase<? extends MtAmazonDynamoDbBase>) mock(MtAmazonDynamoDbStreamsBase.class);
        Map<AmazonDynamoDB, MtAmazonDynamoDbStreamsBase<? extends MtAmazonDynamoDbBase>> streamsPerDelegateDb
            = ImmutableMap.of(first, firstStreams, second, secondStreams);
        MtAmazonDynamoDbStreamsComposite streams = new MtAmazonDynamoDbStreamsComposite(
            mock(AmazonDynamoDBStreams.class), composite, streamsPerDelegateDb);

        CONTEXT.set("1");
        GetRecordsRequest request = mock(GetRecordsRequest.class);
        MtStreamArn mtStreamArn = mock(MtStreamArn.class);
        streams.getRecords(request, mtStreamArn);
        verify(firstStreams).getRecords(request, mtStreamArn);

        CONTEXT.set("2");
        streams.getRecords(request, mtStreamArn);
        verify(secondStreams).getRecords(request, mtStreamArn);

        CONTEXT.set(null);
        StreamArn streamArn = mock(StreamArn.class);
        when(streamArn.getTableName()).thenReturn("table1");
        streams.getAllRecords(request, streamArn);
        verify(firstStreams).getAllRecords(request, streamArn);

        when(streamArn.getTableName()).thenReturn("table2");
        streams.getAllRecords(request, streamArn);
        verify(secondStreams).getAllRecords(request, streamArn);
    }

    private MtAmazonDynamoDbBase getMockMtAmazonDynamoDb(String physicalTable) {
        MtAmazonDynamoDbBase mockMtAmazonDynamoDb = mock(MtAmazonDynamoDbBase.class);
        when(mockMtAmazonDynamoDb.getMtContext()).thenReturn(MT_CONTEXT_PROVIDER);
        when(mockMtAmazonDynamoDb.getAmazonDynamoDb()).thenReturn(amazonDynamoDb);
        when(mockMtAmazonDynamoDb.getMeterRegistry()).thenReturn(meterRegistry);
        when(mockMtAmazonDynamoDb.getScanTenantKey()).thenReturn(SCAN_TENANT_KEY);
        when(mockMtAmazonDynamoDb.getScanVirtualTableKey()).thenReturn(SCAN_VIRTUAL_TABLE_KEY);
        when(mockMtAmazonDynamoDb.isMtTable(eq(physicalTable))).thenReturn(true);
        return mockMtAmazonDynamoDb;
    }

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
        AmazonDynamoDB amazonDynamoDb = mock(AmazonDynamoDB.class);
        MeterRegistry meterRegistry = mock(MeterRegistry.class);
        MtAmazonDynamoDbBase first = new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, amazonDynamoDb, meterRegistry,
            SCAN_TENANT_KEY, SCAN_VIRTUAL_TABLE_KEY);
        MtAmazonDynamoDbBase second = new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, amazonDynamoDb, meterRegistry,
            SCAN_TENANT_KEY, SCAN_VIRTUAL_TABLE_KEY);

        return Stream.of(
            // valid
            Arguments.of(ImmutableList.of(first, second), null),
            // invalid
            Arguments.of(null, "Must provide at least one delegate"),
            Arguments.of(Collections.emptyList(), "Must provide at least one delegate"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(null, amazonDynamoDb, meterRegistry, SCAN_TENANT_KEY,
                    SCAN_VIRTUAL_TABLE_KEY)),
                "Delegates must share the same mt context provider"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, null, meterRegistry, SCAN_TENANT_KEY,
                    SCAN_VIRTUAL_TABLE_KEY)),
                "Delegates must share the same parent AmazonDynamoDB"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, amazonDynamoDb, null, SCAN_TENANT_KEY,
                    SCAN_VIRTUAL_TABLE_KEY)),
                "Delegates must share the same meter registry"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, amazonDynamoDb, meterRegistry, null,
                    SCAN_VIRTUAL_TABLE_KEY)),
                "Delegates must share the same scan tenant key"),
            Arguments.of(ImmutableList.of(first, second,
                new MtAmazonDynamoDbBase(MT_CONTEXT_PROVIDER, amazonDynamoDb, meterRegistry, SCAN_TENANT_KEY,
                    null)),
                "Delegates must share the same scan virtual table key")
        );
    }

}