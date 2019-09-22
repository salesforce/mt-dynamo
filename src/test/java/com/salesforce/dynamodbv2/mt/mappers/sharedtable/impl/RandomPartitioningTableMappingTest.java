/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARY_INDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class RandomPartitioningTableMappingTest {

    private final DynamoTableDescription virtualTable = TableMappingTestUtil.buildTable("virtualTableName",
        new PrimaryKey("virtualHk", S, "virtualRk", N),
        ImmutableMap.of("virtualGsi", new PrimaryKey("virtualGsiHk", S, "virtualGsiRk", N)));
    private final DynamoTableDescription physicalTable = TableMappingTestUtil.buildTable("physicalTableName",
        new PrimaryKey("physicalHk", S, "physicalRk", N),
        ImmutableMap.of("physicalGsi", new PrimaryKey("physicalGsiHk", S, "physicalGsiRk", N)));
    private final RandomPartitioningTableMapping sut = new RandomPartitioningTableMapping(virtualTable,
        physicalTable,
        index -> index.getIndexName().equals("virtualGsi") ? physicalTable.findSi("physicalGsi") : null,
        null
    );
    private final Map<String, List<FieldMapping>> virtualToPhysicalFieldMappings = ImmutableMap.of(
        "virtualHk", ImmutableList.of(
            new FieldMapping(new Field("virtualHk", S),
                new Field("physicalHk", S),
                "virtualTableName",
                "physicalTableName",
                TABLE,
                true)),
        "virtualRk", ImmutableList.of(
            new FieldMapping(new Field("virtualRk", N),
                new Field("physicalRk", N),
                "virtualTableName",
                "physicalTableName",
                TABLE,
                false)),
        "virtualGsiHk", ImmutableList.of(new FieldMapping(new Field("virtualGsiHk", S),
            new Field("physicalGsiHk", S),
            "virtualGsi",
            "physicalGsi",
            SECONDARY_INDEX,
            true)),
        "virtualGsiRk", ImmutableList.of(new FieldMapping(new Field("virtualGsiRk", N),
            new Field("physicalGsiRk", N),
            "virtualGsi",
            "physicalGsi",
            SECONDARY_INDEX,
            false)));

    @Test
    void getVirtualTable() {
        assertEquals(virtualTable, sut.getVirtualTable());
    }

    @Test
    void getPhysicalTable() {
        assertEquals(physicalTable, sut.getPhysicalTable());
    }

    @Test
    void getItemMapper() {
        assertNotNull(sut.getItemMapper());
    }

    @Test
    void getQueryMapper() {
        assertNotNull(sut.getQueryAndScanMapper());
    }

    @Test
    void getIndexPrimaryKeyFieldMappings() {
        assertEquals(virtualToPhysicalFieldMappings.entrySet().stream()
                .filter(fieldMappingEntry -> fieldMappingEntry.getKey().contains("Gsi"))
                .flatMap((Function<Entry<String, List<FieldMapping>>, Stream<FieldMapping>>)
                    fieldMappingEntry -> fieldMappingEntry.getValue().stream())
                .collect(Collectors.toList()),
            sut.getIndexPrimaryKeyFieldMappings(virtualTable.getGsi("virtualGsi").orElseThrow()));
    }

    @Test
    void getTablePrimaryKeyFieldMappings() {
        assertEquals(virtualToPhysicalFieldMappings.entrySet().stream()
                .filter(fieldMappingEntry -> !fieldMappingEntry.getKey().contains("Gsi"))
                .flatMap((Function<Entry<String, List<FieldMapping>>, Stream<FieldMapping>>)
                    fieldMappingEntry -> fieldMappingEntry.getValue().stream())
                .collect(Collectors.toList()),
            sut.getTablePrimaryKeyFieldMappings());
    }

    private static CreateTableRequestBuilder buildDefaultCreateTableRequestBuilder() {
        return CreateTableRequestBuilder
            .builder()
            .withTableName("virtualTableName")
            .withTableKeySchema("virtualHk", S, "virtualRk", N);
    }

    private static CreateTableRequestBuilder buildDefaultCreateTableRequestBuilderWithGsi() {
        return buildDefaultCreateTableRequestBuilder()
            .addSi("virtualGsi",
                GSI,
                new PrimaryKey("virtualGsiHk", S, "virtualGsiRk", N),
                1L);
    }

}