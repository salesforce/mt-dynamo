/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARY_INDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.lang.String.format;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * {@link TableMapping} implementation for shared tables that use random partitioning. That is, where
 * <pre>
 *     physicalHashKey = tenantId + virtualTableName + virtualHashKey
 *     physicalRangeKey = virtualRangeKey
 * </pre>
 * See https://salesforce.quip.com/hULMAJ0KNFUY
 *
 * @author msgroi
 */
public class RandomPartitioningTableMapping implements TableMapping {

    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;
    private final UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper;

    private final List<FieldMapping> tablePrimaryKeyFieldMappings;
    private final Map<DynamoSecondaryIndex, List<FieldMapping>> indexPrimaryKeyFieldMappings;
    private final Map<String, List<FieldMapping>> allMappingsPerField;

    private final ItemMapper itemMapper;
    private final RecordMapper recordMapper;
    private final QueryAndScanMapper queryAndScanMapper;
    private final ConditionMapper conditionMapper;

    public RandomPartitioningTableMapping(DynamoTableDescription virtualTable,
                                          DynamoTableDescription physicalTable,
                                          UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                          MtAmazonDynamoDbContextProvider mtContext) {
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.secondaryIndexMapper = secondaryIndexMapper;
        this.tablePrimaryKeyFieldMappings = buildTablePrimaryKeyFieldMappings(virtualTable, physicalTable);
        this.indexPrimaryKeyFieldMappings = buildIndexPrimaryKeyFieldMappings(virtualTable, secondaryIndexMapper);

        // build map from each field to any PK or secondary index mappings
        this.allMappingsPerField = new HashMap<>();
        tablePrimaryKeyFieldMappings.forEach(
            fieldMapping -> addFieldMapping(allMappingsPerField, fieldMapping));
        indexPrimaryKeyFieldMappings.values().forEach(
            list -> list.forEach(
                fieldMapping -> addFieldMapping(allMappingsPerField, fieldMapping)));

        FieldMapper fieldMapper = physicalTable.getPrimaryKey().getHashKeyType() == S
            ? new StringFieldMapper(mtContext, virtualTable.getTableName())
            : new BinaryFieldMapper(mtContext, virtualTable.getTableName());

        itemMapper = new RandomPartitioningItemMapper(fieldMapper, tablePrimaryKeyFieldMappings,
            indexPrimaryKeyFieldMappings, allMappingsPerField);
        recordMapper = new RandomPartitioningRecordMapper(mtContext, virtualTable.getTableName(), itemMapper,
            fieldMapper, physicalTable.getPrimaryKey().getHashKey());
        conditionMapper = new RandomPartitioningConditionMapper(this, fieldMapper);
        queryAndScanMapper = new RandomPartitioningQueryAndScanMapper(virtualTable, this::getRequestIndex,
            conditionMapper, itemMapper, fieldMapper, tablePrimaryKeyFieldMappings, indexPrimaryKeyFieldMappings);
    }

    @Override
    public DynamoTableDescription getVirtualTable() {
        return virtualTable;
    }

    @Override
    public DynamoTableDescription getPhysicalTable() {
        return physicalTable;
    }

    @Override
    public ItemMapper getItemMapper() {
        return itemMapper;
    }

    @Override
    public RecordMapper getRecordMapper() {
        return recordMapper;
    }

    @Override
    public QueryAndScanMapper getQueryAndScanMapper() {
        return queryAndScanMapper;
    }

    @Override
    public ConditionMapper getConditionMapper() {
        return conditionMapper;
    }

    @Override
    public RequestIndex getRequestIndex(@Nullable String virtualSecondaryIndexName) {
        return RequestIndex.fromVirtualSecondaryIndexName(virtualTable, physicalTable, secondaryIndexMapper,
            virtualSecondaryIndexName);
    }

    /*
     * Returns a mapping of virtual to physical fields for the table primary key
     */
    List<FieldMapping> getTablePrimaryKeyFieldMappings() {
        return tablePrimaryKeyFieldMappings;
    }

    /*
     * Returns a mapping of primary key fields for a specific secondary index, virtual to physical.
     */
    List<FieldMapping> getIndexPrimaryKeyFieldMappings(DynamoSecondaryIndex virtualSecondaryIndex) {
        return indexPrimaryKeyFieldMappings.get(virtualSecondaryIndex);
    }

    /*
     * Returns map from each field to any PK or secondary index mappings.
     */
    Map<String, List<FieldMapping>> getAllMappingsPerField() {
        return allMappingsPerField;
    }

    @Override
    public String toString() {
        return format("%s -> %s, virtual: %s, physical: %s, tableFieldMappings: %s, secondaryIndexFieldMappings: %s",
            getVirtualTable().getTableName(), getPhysicalTable().getTableName(),
            getVirtualTable().toString(), getPhysicalTable().toString(),
            tablePrimaryKeyFieldMappings, indexPrimaryKeyFieldMappings);
    }

    /*
     * Returns a mapping of table-level primary key fields only, virtual to physical.
     */
    private static List<FieldMapping> buildTablePrimaryKeyFieldMappings(DynamoTableDescription virtualTable,
                                                                        DynamoTableDescription physicalTable) {
        List<FieldMapping> fieldMappings = new ArrayList<>();
        fieldMappings.add(new FieldMapping(new Field(virtualTable.getPrimaryKey().getHashKey(),
            virtualTable.getPrimaryKey().getHashKeyType()),
            new Field(physicalTable.getPrimaryKey().getHashKey(),
                physicalTable.getPrimaryKey().getHashKeyType()),
            virtualTable.getTableName(),
            physicalTable.getTableName(),
            TABLE,
            true));
        if (virtualTable.getPrimaryKey().getRangeKey().isPresent()) {
            fieldMappings.add(new FieldMapping(new Field(virtualTable.getPrimaryKey().getRangeKey().get(),
                virtualTable.getPrimaryKey().getRangeKeyType().orElseThrow()),
                new Field(physicalTable.getPrimaryKey().getRangeKey().orElseThrow(),
                    physicalTable.getPrimaryKey().getRangeKeyType().orElseThrow()),
                virtualTable.getTableName(),
                physicalTable.getTableName(),
                TABLE,
                false));
        }
        return fieldMappings;
    }

    private static Map<DynamoSecondaryIndex, List<FieldMapping>> buildIndexPrimaryKeyFieldMappings(
        DynamoTableDescription virtualTable,
        UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper) {
        Map<DynamoSecondaryIndex, List<FieldMapping>> secondaryIndexFieldMappings = new HashMap<>();
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            List<FieldMapping> fieldMappings = new ArrayList<>();
            DynamoSecondaryIndex physicalSi = secondaryIndexMapper.apply(virtualSi);
            fieldMappings.add(new FieldMapping(new Field(virtualSi.getPrimaryKey().getHashKey(),
                virtualSi.getPrimaryKey().getHashKeyType()),
                new Field(physicalSi.getPrimaryKey().getHashKey(),
                    physicalSi.getPrimaryKey().getHashKeyType()),
                virtualSi.getIndexName(),
                physicalSi.getIndexName(),
                virtualSi.getType() == LSI ? TABLE : SECONDARY_INDEX,
                true));
            if (virtualSi.getPrimaryKey().getRangeKey().isPresent()) {
                fieldMappings.add(new FieldMapping(new Field(virtualSi.getPrimaryKey().getRangeKey().get(),
                    virtualSi.getPrimaryKey().getRangeKeyType().orElseThrow()),
                    new Field(physicalSi.getPrimaryKey().getRangeKey().orElseThrow(),
                        physicalSi.getPrimaryKey().getRangeKeyType().orElseThrow()),
                    virtualSi.getIndexName(),
                    physicalSi.getIndexName(),
                    SECONDARY_INDEX,
                    false));
            }
            secondaryIndexFieldMappings.put(virtualSi, fieldMappings);
        }
        return secondaryIndexFieldMappings;
    }

    /**
     * Helper method for adding a single FieldMapping object to the existing list of FieldMapping objects.
     */
    private static void addFieldMapping(Map<String, List<FieldMapping>> fieldMappings, FieldMapping fieldMappingToAdd) {
        String key = fieldMappingToAdd.getSource().getName();
        List<FieldMapping> fieldMapping = fieldMappings.computeIfAbsent(key, k -> new ArrayList<>());
        fieldMapping.add(fieldMappingToAdd);
    }

}