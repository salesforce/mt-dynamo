/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARYINDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.util.Optional.ofNullable;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Holds the state of mapping of a virtual table to a physical table.  It provides methods for retrieving the virtual
 * and physical descriptions, the mapping of fields from virtual to physical and back.
 *
 * @author msgroi
 */
class TableMapping {

    private final DynamoTableDescription virtualTable;
    private DynamoTableDescription physicalTable;
    private final DynamoSecondaryIndexMapper secondaryIndexMapper;
    private final Map<String, List<FieldMapping>> virtualToPhysicalMappings;
    private final Map<String, List<FieldMapping>> physicalToVirtualMappings;
    private final Map<DynamoSecondaryIndex, List<FieldMapping>> secondaryIndexFieldMappings;

    private final ItemMapper itemMapper;
    private final QueryMapper queryMapper;
    private final ConditionMapper conditionMapper;

    TableMapping(DynamoTableDescription virtualTable,
                 CreateTableRequestFactory createTableRequestFactory,
                 DynamoSecondaryIndexMapper secondaryIndexMapper,
                 MtAmazonDynamoDbContextProvider mtContext,
                 String delimiter) {
        physicalTable = lookupPhysicalTable(virtualTable, createTableRequestFactory);
        validatePhysicalTable(physicalTable);
        this.secondaryIndexMapper = secondaryIndexMapper;
        this.virtualTable = virtualTable;
        this.secondaryIndexFieldMappings =
            buildIndexPrimaryKeyFieldMappings(virtualTable, physicalTable, secondaryIndexMapper);
        this.virtualToPhysicalMappings = buildAllVirtualToPhysicalFieldMappings(virtualTable);
        this.physicalToVirtualMappings = buildAllPhysicalToVirtualFieldMappings(virtualToPhysicalMappings);
        validateVirtualPhysicalCompatibility();
        FieldMapper fieldMapper = new FieldMapper(mtContext,
            virtualTable.getTableName(),
            new FieldPrefixFunction(delimiter));
        itemMapper = new ItemMapper(this, fieldMapper);
        queryMapper = new QueryMapper(this, fieldMapper);
        conditionMapper = new ConditionMapper(this, fieldMapper);
    }

    DynamoTableDescription getVirtualTable() {
        return virtualTable;
    }

    DynamoTableDescription getPhysicalTable() {
        return physicalTable;
    }

    ItemMapper getItemMapper() {
        return itemMapper;
    }

    QueryMapper getQueryMapper() {
        return queryMapper;
    }

    ConditionMapper getConditionMapper() {
        return conditionMapper;
    }

    /*
     * Returns a mapping of virtual to physical fields.
     */
    Map<String, List<FieldMapping>> getAllVirtualToPhysicalFieldMappings() {
        return virtualToPhysicalMappings;
    }

    /*
     * Returns a mapping of virtual to physical fields.  When a virtual field maps to more than one physical field
     * then those mappings are reduced to one by selecting one arbitrarily.  See dedupeFieldMappings() method.
     */
    Map<String, FieldMapping> getAllVirtualToPhysicalFieldMappingsDeduped() {
        return dedupeFieldMappings(virtualToPhysicalMappings);
    }

    /*
     * This method takes a mapping of virtual to physical fields, where it is possible that a single given virtual
     * field may map to more than one physical field, and returns a mapping where each virtual field maps to exactly
     * one physical field.  In cases where there is more than one physical field for a given virtual field, it
     * arbitrarily chooses the first mapping.
     *
     * This method is called for any query or scan request that does not specify an index.
     *
     * It is an effective no-op, meaning, there are no duplicates to remove, except when a scan is performed against
     * a table that maps a given virtual field to multiple physical fields.  In that case, it doesn't matter which
     * field we use in the query, the results should be the same, so we choose one of the physical fields arbitrarily.
     */
    private static Map<String, FieldMapping> dedupeFieldMappings(Map<String, List<FieldMapping>> fieldMappings) {
        return fieldMappings.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey,
            fieldMappingEntry -> fieldMappingEntry.getValue().get(0)
        ));
    }

    /*
     * Returns a mapping of physical to virtual fields.
     */
    Map<String, List<FieldMapping>> getAllPhysicalToVirtualFieldMappings() {
        return physicalToVirtualMappings;
    }

    /*
     * Returns a mapping of primary key fields for a specific secondary index, virtual to physical.
     */
    List<FieldMapping> getIndexPrimaryKeyFieldMappings(DynamoSecondaryIndex virtualSecondaryIndex) {
        return secondaryIndexFieldMappings.get(virtualSecondaryIndex);
    }

    /*
     * Returns a mapping of table-level primary key fields only, virtual to physical.
     */
    private List<FieldMapping> getTablePrimaryKeyFieldMappings() {
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
                virtualTable.getPrimaryKey().getRangeKeyType().get()),
                new Field(physicalTable.getPrimaryKey().getRangeKey().get(),
                    physicalTable.getPrimaryKey().getRangeKeyType().get()),
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

    private Map<String, List<FieldMapping>> buildAllVirtualToPhysicalFieldMappings(
        DynamoTableDescription virtualTable) {
        Map<String, List<FieldMapping>> fieldMappings = new HashMap<>();
        getTablePrimaryKeyFieldMappings().forEach(fieldMapping -> addFieldMapping(fieldMappings, fieldMapping));
        virtualTable.getSis().forEach(virtualSi -> getIndexPrimaryKeyFieldMappings(virtualSi)
            .forEach(fieldMapping -> addFieldMapping(fieldMappings, fieldMapping)));
        return fieldMappings;
    }

    private Map<String, List<FieldMapping>> buildAllPhysicalToVirtualFieldMappings(
        Map<String, List<FieldMapping>> virtualToPhysicalMappings) {
        Map<String, List<FieldMapping>> fieldMappings = new HashMap<>();
        virtualToPhysicalMappings.values().stream()
            .flatMap((Function<List<FieldMapping>, Stream<FieldMapping>>) Collection::stream)
            .forEach(fieldMapping -> fieldMappings.put(fieldMapping.getTarget().getName(),
                ImmutableList.of(new FieldMapping(fieldMapping.getTarget(),
                    fieldMapping.getSource(),
                    fieldMapping.getVirtualIndexName(),
                    fieldMapping.getPhysicalIndexName(),
                    fieldMapping.getIndexType(),
                    fieldMapping.isContextAware()))));
        return fieldMappings;
    }

    private Map<DynamoSecondaryIndex, List<FieldMapping>> buildIndexPrimaryKeyFieldMappings(
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
                    virtualSi.getType() == LSI ? TABLE : SECONDARYINDEX,
                    true));
                if (virtualSi.getPrimaryKey().getRangeKey().isPresent()) {
                    fieldMappings.add(new FieldMapping(new Field(virtualSi.getPrimaryKey().getRangeKey().get(),
                        virtualSi.getPrimaryKey().getRangeKeyType().get()),
                        new Field(physicalSi.getPrimaryKey().getRangeKey().get(),
                            physicalSi.getPrimaryKey().getRangeKeyType().get()),
                        virtualSi.getIndexName(),
                        physicalSi.getIndexName(),
                        SECONDARYINDEX,
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
     * Helper method for adding a single FieldMapping to the existing list of FieldMapping's.
     */
    private void addFieldMapping(Map<String, List<FieldMapping>> fieldMappings, FieldMapping fieldMappingToAdd) {
        String key = fieldMappingToAdd.getSource().getName();
        List<FieldMapping> fieldMapping = fieldMappings.computeIfAbsent(key, k -> new ArrayList<>());
        fieldMapping.add(fieldMappingToAdd);
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

    @VisibleForTesting
    void validateSecondaryIndexes(DynamoTableDescription virtualTable,
                                  DynamoTableDescription physicalTable,
                                  DynamoSecondaryIndexMapper secondaryIndexMapper) {
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            DynamoSecondaryIndex physicalSi;
            // map the virtual index a physical one
            try {
                physicalSi = secondaryIndexMapper.lookupPhysicalSecondaryIndex(virtualSi, physicalTable);
            } catch (IllegalArgumentException | NullPointerException | MappingException e) {
                throw new IllegalArgumentException("failure mapping virtual to physical " + virtualSi.getType()
                    + ": " + e.getMessage() + ", virtualSiPrimaryKey=" + virtualSi + ", virtualTable=" + virtualTable
                    + ", physicalTable=" + physicalTable);
            }
            try {
                // validate each virtual against the physical index that it was mapped to
                validateCompatiblePrimaryKey(virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey());
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new IllegalArgumentException("invalid mapping virtual to physical " + virtualSi.getType()
                    + ": " + e.getMessage() + ", virtualSiPrimaryKey=" + virtualSi.getPrimaryKey()
                    + ", physicalSiPrimaryKey=" + physicalSi.getPrimaryKey()
                    + ", virtualTable=" + virtualTable + ", physicalTable=" + physicalTable);
            }
        }

        validateLsiMappings(virtualTable, physicalTable, secondaryIndexMapper);
    }

    /*
     * Validate that for any given physical LSI, there is no more than one virtual LSI that is mapped to it.
     */
    private void validateLsiMappings(DynamoTableDescription virtualTable,
                                     DynamoTableDescription physicalTable,
                                     DynamoSecondaryIndexMapper secondaryIndexMapper) {
        Map<DynamoSecondaryIndex, DynamoSecondaryIndex> usedPhysicalLsis = new HashMap<>();
        virtualTable.getLsis().forEach(virtualLsi -> {
            try {
                DynamoSecondaryIndex physicalLsi = secondaryIndexMapper.lookupPhysicalSecondaryIndex(virtualLsi,
                                                                                                     physicalTable);
                checkArgument(!usedPhysicalLsis.containsKey(physicalLsi),
                    "two virtual LSI's(one:" + usedPhysicalLsis.get(physicalLsi) + ", two:"
                        + virtualLsi + ", mapped to one physical LSI: " + physicalLsi);
                usedPhysicalLsis.put(physicalLsi, virtualLsi);
            } catch (MappingException e) {
                throw new IllegalArgumentException("failure mapping virtual to physical " + virtualLsi.getType() + ": "
                    + e.getMessage() + ", virtualSiPrimaryKey=" + virtualLsi + ", virtualTable=" + virtualTable
                    + ", physicalTable=" + physicalTable);
            }
        });
    }

    /*
     * Validates that virtual and physical indexes have hash keys with matching types.  If there is a range key on the
     * virtual index, then it also validates that the physical index also has one and their types match.
     */
    @VisibleForTesting
    void validateCompatiblePrimaryKey(PrimaryKey virtualPrimaryKey, PrimaryKey physicalPrimaryKey)
        throws IllegalArgumentException, NullPointerException {
        checkNotNull(virtualPrimaryKey.getHashKey(), "hashkey is required on virtual table");
        checkNotNull(physicalPrimaryKey.getHashKey(), "hashkey is required on physical table");
        checkArgument(physicalPrimaryKey.getHashKeyType() == S, "hashkey must be of type S");
        if (virtualPrimaryKey.getRangeKey().isPresent()) {
            checkArgument(physicalPrimaryKey.getRangeKey().isPresent(),
                          "rangeKey exists on virtual primary key but not on physical");
            checkArgument(virtualPrimaryKey.getRangeKeyType().get() == physicalPrimaryKey.getRangeKeyType().get(),
                          "virtual and physical rangekey types mismatch");
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
        checkArgument(primaryKey.getHashKeyType() == S,
            msgPrefix + " primary key hashkey must be type S, encountered type "
                + primaryKey.getHashKeyType());
    }

    void setPhysicalTable(DynamoTableDescription physicalTable) {
        this.physicalTable = physicalTable;
    }

}