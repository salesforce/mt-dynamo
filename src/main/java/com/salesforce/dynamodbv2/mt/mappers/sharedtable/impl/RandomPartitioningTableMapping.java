/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARY_INDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.lang.String.format;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapper;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link TableMapping} implementation for shared tables that use random partitioning. That is, where
 * <pre>
 *     physicalHashKey = tenantId + virtualTableName + virtualHashKey
 *     phyiscalRangeKey = virtualRangeKey
 * </pre>
 * See https://salesforce.quip.com/hULMAJ0KNFUY
 *
 * @author msgroi
 */
public class RandomPartitioningTableMapping implements TableMapping {

    private final DynamoTableDescription virtualTable;
    private DynamoTableDescription physicalTable;
    private final DynamoSecondaryIndexMapper secondaryIndexMapper;
    private final List<FieldMapping> tablePrimaryKeyFieldMappings;
    private final Map<DynamoSecondaryIndex, List<FieldMapping>> indexPrimaryKeyFieldMappings;

    private final ItemMapper itemMapper;
    private final RecordMapper recordMapper;
    private final QueryAndScanMapper queryAndScanMapper;
    private final ConditionMapper conditionMapper;

    RandomPartitioningTableMapping(DynamoTableDescription virtualTable,
                                   CreateTableRequestFactory createTableRequestFactory,
                                   DynamoSecondaryIndexMapper secondaryIndexMapper,
                                   MtAmazonDynamoDbContextProvider mtContext) {
        physicalTable = lookupPhysicalTable(virtualTable, createTableRequestFactory);
        validatePhysicalTable(physicalTable);
        this.secondaryIndexMapper = secondaryIndexMapper;
        this.virtualTable = virtualTable;
        validateMapping();

        this.tablePrimaryKeyFieldMappings = buildTablePrimaryKeyFieldMappings(virtualTable, physicalTable);
        this.indexPrimaryKeyFieldMappings =
            buildIndexPrimaryKeyFieldMappings(virtualTable, physicalTable, secondaryIndexMapper);

        FieldMapper fieldMapper = physicalTable.getPrimaryKey().getHashKeyType() == S
            ? new StringFieldMapper(mtContext, virtualTable.getTableName())
            : new BinaryFieldMapper(mtContext, virtualTable.getTableName());

        itemMapper = new RandomPartitioningItemMapper(fieldMapper, tablePrimaryKeyFieldMappings,
            indexPrimaryKeyFieldMappings);
        recordMapper = new RandomPartitioningRecordMapper(mtContext, virtualTable.getTableName(), itemMapper,
            fieldMapper, physicalTable.getPrimaryKey().getHashKey());
        queryAndScanMapper = new RandomPartitioningQueryAndScanMapper(this, fieldMapper);
        conditionMapper = new RandomPartitioningConditionMapper(this, fieldMapper);
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

    @Override
    public String toString() {
        return format("%s -> %s, virtual: %s, physical: %s, tableFieldMappings: %s, secondaryIndexFieldMappings: %s",
            virtualTable.getTableName(), physicalTable.getTableName(),
            virtualTable.toString(), physicalTable.toString(),
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

    /*
     * Calls the provided CreateTableRequestFactory passing in the virtual table description and returns the
     * corresponding physical table.  Throws a ResourceNotFoundException if the implementation returns null.
     */
    private DynamoTableDescription lookupPhysicalTable(DynamoTableDescription virtualTable,
                                                       CreateTableRequestFactory createTableRequestFactory) {
        return new DynamoTableDescriptionImpl(
            createTableRequestFactory.getCreateTableRequest(virtualTable)
            .orElseThrow((Supplier<ResourceNotFoundException>) () ->
                new ResourceNotFoundException("table " + virtualTable.getTableName() + " is not a supported table")));
    }

    private static Map<DynamoSecondaryIndex, List<FieldMapping>> buildIndexPrimaryKeyFieldMappings(
        DynamoTableDescription virtualTable,
        DynamoTableDescription physicalTable,
        DynamoSecondaryIndexMapper secondaryIndexMapper) {
        Map<DynamoSecondaryIndex, List<FieldMapping>> secondaryIndexFieldMappings = new HashMap<>();
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            List<FieldMapping> fieldMappings = new ArrayList<>();
            try {
                DynamoSecondaryIndex physicalSi = secondaryIndexMapper.lookupPhysicalSecondaryIndex(virtualSi,
                    physicalTable);
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
            } catch (MappingException e) {
                throw new IllegalArgumentException("failure mapping virtual to physical " + virtualSi.getType()
                    + ": " + e.getMessage() + ", virtualSiPrimaryKey=" + virtualSi + ", virtualTable=" + virtualTable
                    + ", physicalTable=" + physicalTable);
            }
        }
        return secondaryIndexFieldMappings;
    }

    /*
     * Validates the table mapping.
     */
    private void validateMapping() {
        validateVirtualPhysicalCompatibility();
    }

    /*
     * Validate that the key schema elements match between the table's virtual and physical primary key as
     * well as indexes.
     */
    private void validateVirtualPhysicalCompatibility() {
        // validate primary key
        try {
            validateCompatiblePrimaryKey(virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey());
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException("invalid mapping virtual to physical table primary key: "
                + e.getMessage() + ", virtualTable=" + virtualTable + ", physicalTable=" + physicalTable);
        }

        // validate secondary indexes
        validateSecondaryIndexes(virtualTable, physicalTable, secondaryIndexMapper);
    }

    /**
     * Validates that each virtual secondary index can be mapped to a physical secondary index, throwing an
     * IllegalArgumentException if an index can't be mapped.  Also validates that no physical
     * secondary index has more than one virtual secondary index mapped to it, throwing an IllegalStateException
     * if this state is encountered.
     */
    @VisibleForTesting
    void validateSecondaryIndexes(DynamoTableDescription virtualTable,
                                  DynamoTableDescription physicalTable,
                                  DynamoSecondaryIndexMapper secondaryIndexMapper) {
        final List<DynamoSecondaryIndex> secondaryIndexes = virtualTable.getSis();
        checkArgument(secondaryIndexes.size() == secondaryIndexes.stream().map(virtualSi -> {
            try {
                return secondaryIndexMapper.lookupPhysicalSecondaryIndex(virtualSi, physicalTable);
            } catch (MappingException e) {
                throw new IllegalArgumentException("failure mapping virtual to physical " + virtualSi.getType()
                    + ": " + e.getMessage() + ", virtualSiPrimaryKey=" + virtualSi + ", virtualTable="
                    + virtualTable + ", physicalTable=" + physicalTable);
            }
        }).collect(Collectors.toSet()).size(),
            "More than one virtual secondary index maps to the same physical secondary index");
    }

    /*
     * Validates that virtual and physical indexes have hash keys with matching types.  If there is a range key on the
     * virtual index, then it also validates that the physical index also has one and their types match.
     */
    @VisibleForTesting
    void validateCompatiblePrimaryKey(PrimaryKey virtualPrimaryKey, PrimaryKey physicalPrimaryKey)
        throws IllegalArgumentException, NullPointerException {
        checkNotNull(virtualPrimaryKey.getHashKey(), "hash key is required on virtual table");
        checkNotNull(physicalPrimaryKey.getHashKey(), "hash key is required on physical table");
        checkArgument(physicalPrimaryKey.getHashKeyType() == S || physicalPrimaryKey.getHashKeyType() == B,
            "hash key must be of type S or B");
        if (virtualPrimaryKey.getRangeKey().isPresent()) {
            checkArgument(physicalPrimaryKey.getRangeKey().isPresent(),
                          "rangeKey exists on virtual primary key but not on physical");
            checkArgument(virtualPrimaryKey.getRangeKeyType().orElseThrow()
                    == physicalPrimaryKey.getRangeKeyType().orElseThrow(),
                          "virtual and physical range-key types mismatch");
        }
    }

    /*
     * Validate that the physical table's primary key and all of its secondary index's primary keys are of type S.
     */
    @VisibleForTesting
    void validatePhysicalTable(DynamoTableDescription physicalTableDescription) {
        String tableMsgPrefix = "physical table " + physicalTableDescription.getTableName() + "'s";
        validatePrimaryKey(physicalTableDescription.getPrimaryKey(), tableMsgPrefix);
        physicalTableDescription.getGsis().forEach(dynamoSecondaryIndex ->
            validatePrimaryKey(dynamoSecondaryIndex.getPrimaryKey(), tableMsgPrefix
                               + " GSI " + dynamoSecondaryIndex.getIndexName() + "'s"));
        physicalTableDescription.getLsis().forEach(dynamoSecondaryIndex ->
            validatePrimaryKey(dynamoSecondaryIndex.getPrimaryKey(), tableMsgPrefix
                               + " LSI " + dynamoSecondaryIndex.getIndexName() + "'s"));
    }

    private void validatePrimaryKey(PrimaryKey primaryKey, String msgPrefix) {
        checkArgument(primaryKey.getHashKeyType() == S || primaryKey.getHashKeyType() == B,
            msgPrefix + " primary-key hash key must be type S, encountered type "
                + primaryKey.getHashKeyType());
    }

    void setPhysicalTable(DynamoTableDescription physicalTable) {
        this.physicalTable = physicalTable;
    }

}