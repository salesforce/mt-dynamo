/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperTrackingAssigned;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.metadata.VirtualDynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.VirtualDynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@code TableMapping}s that contain the state of given mapping of a virtual table to a physical table.  The
 * {@code TableMapping} also includes methods for retrieving the virtual and physical descriptions, and logic for
 * mapping of fields from virtual to physical and back.
 *
 * <p>This class is also responsible for triggering the creation of the physical tables appropriately.
 *
 * @author msgroi
 */
public class TableMappingFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TableMappingFactory.class);

    private final CreateTableRequestFactory createTableRequestFactory;
    private final PhysicalTableManager physicalTableManager;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final TablePartitioningStrategy partitioningStrategy;
    private final VirtualTableCreationValidator virtualTableCreationValidator;

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequestFactory maps virtual to physical table instances
     * @param physicalTableManager      provides physical table admin, i.e., create, delete, describe
     * @param mtContext                 the multitenant context provider
     * @param partitioningStrategy      how data are partitioned in tables
     * @param createTablesEagerly       a flag indicating whether to create physical tables eagerly at start time
     */
    public TableMappingFactory(CreateTableRequestFactory createTableRequestFactory,
                               PhysicalTableManager physicalTableManager,
                               MtAmazonDynamoDbContextProvider mtContext,
                               TablePartitioningStrategy partitioningStrategy,
                               boolean createTablesEagerly) {
        this.createTableRequestFactory = createTableRequestFactory;
        this.physicalTableManager = physicalTableManager;
        this.mtContext = mtContext;
        this.partitioningStrategy = partitioningStrategy;
        this.virtualTableCreationValidator = new VirtualTableCreationValidator(partitioningStrategy);
        if (createTablesEagerly) {
            createTablesEagerly(createTableRequestFactory, physicalTableManager);
        }
    }

    CreateTableRequestFactory getCreateTableRequestFactory() {
        return createTableRequestFactory;
    }

    private static void createTablesEagerly(CreateTableRequestFactory createTableRequestFactory,
                                            PhysicalTableManager physicalTableManager) {
        createTableRequestFactory.getStaticPhysicalTables().forEach(physicalTableManager::createTableIfNotExists);
    }

    /**
     * Creates the table mapping for a new virtual table, creating the physical table if it doesn't exist.
     *
     */
    TableMapping createNewTableMapping(CreateTableRequest virtualCreateTableRequest, boolean isMultitenant) {
        VirtualDynamoTableDescription virtualTable = new VirtualDynamoTableDescriptionImpl(virtualCreateTableRequest,
            isMultitenant);
        return getTableMapping(virtualTable, true);
    }

    /**
     * Creates the table mapping, looking up and setting the physical table description back onto the table mapping
     * so it includes things that can only be determined after the physical table is created, like the streamArn.
     */
    TableMapping getTableMapping(VirtualDynamoTableDescription virtualTable) {
        return getTableMapping(virtualTable, false);
    }

    private TableMapping getTableMapping(VirtualDynamoTableDescription virtualTable,
                                         boolean createPhysicalTableIfNotExists) {
        CreateTableRequest physicalCreateTableRequest = lookupPhysicalTable(virtualTable);
        DynamoTableDescription physicalTable = new DynamoTableDescriptionImpl(physicalCreateTableRequest);

        // validate physical table key types
        virtualTableCreationValidator.validatePhysicalTable(physicalTable);

        // validate primary key types are compatible
        virtualTableCreationValidator.validateCompatiblePrimaryKeys(virtualTable, physicalTable);

        // validate secondary indexes
        Map<DynamoSecondaryIndex, DynamoSecondaryIndex> secondaryIndexMap =
            virtualTableCreationValidator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable);

        // create the physical table for a new virtual table, or describe the physical table for an existing virtual
        // table. set the returned physical table description back onto the table mapping, so it includes things that
        // can only be determined after the physical table is created, like the streamArn.
        physicalTable = createPhysicalTableIfNotExists
            ? physicalTableManager.createTableIfNotExists(physicalCreateTableRequest)
            : physicalTableManager.describeTable(physicalTable.getTableName());

        TableMapping tableMapping = partitioningStrategy.createTableMapping(virtualTable, physicalTable,
            secondaryIndexMap::get, mtContext);
        LOG.debug("created virtual to physical table mapping: " + tableMapping.toString());
        return tableMapping;
    }

    /**
     * Calls the provided CreateTableRequestFactory passing in the virtual table description and returns the
     * corresponding physical table.  Throws a ResourceNotFoundException if the implementation returns null.
     */
    private CreateTableRequest lookupPhysicalTable(VirtualDynamoTableDescription virtualTable) {
        // if this is a multitenant table, then create a new physical table dedicated to this purpose.
        // otherwise, find the corresponding static physical table.
        return virtualTable.isMultitenant()
            ? createTableRequestFactory.getDynamicPhysicalTable(virtualTable)
            :  createTableRequestFactory.getStaticPhysicalTable(virtualTable).orElseThrow(() ->
            new ResourceNotFoundException("table " + virtualTable.getTableName() + " is not a supported table"));
    }

    @VisibleForTesting
    static class VirtualTableCreationValidator {

        private final TablePartitioningStrategy partitioningStrategy;

        VirtualTableCreationValidator(TablePartitioningStrategy partitioningStrategy) {
            this.partitioningStrategy = partitioningStrategy;
        }

        /**
         * Maps each virtual secondary index to a corresponding physical secondary index, based on our secondary index
         * PrimaryKeyMapper. For each virtual index, the PrimaryKeyMapper will look for a valid physical index amongst
         * the physical indexes that are still unassigned. An IllegalArgumentException is thrown if there is an index
         * that cannot be mapped.
         */
        Map<DynamoSecondaryIndex, DynamoSecondaryIndex> validateAndGetSecondaryIndexMap(
            DynamoTableDescription virtualTable, DynamoTableDescription physicalTable) {
            DynamoSecondaryIndexMapperTrackingAssigned indexMapper = new DynamoSecondaryIndexMapperTrackingAssigned(
                partitioningStrategy.getSecondaryIndexPrimaryKeyMapper());

            for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
                try {
                    indexMapper.lookupPhysicalSecondaryIndex(virtualSi, physicalTable);
                } catch (MappingException e) {
                    throw new IllegalArgumentException("failure mapping virtual " + virtualSi.getType()
                        + ": " + e.getMessage() + ", virtualSi=" + virtualSi
                        + ", virtualTable=" + virtualTable + ", physicalTable=" + physicalTable);
                }
            }
            return indexMapper.getAssignedVirtualToPhysicalIndexes();
        }

        /**
         * Validates that virtual and physical table primary keys are compatible.
         */
        void validateCompatiblePrimaryKeys(DynamoTableDescription virtualTable, DynamoTableDescription physicalTable) {
            try {
                partitioningStrategy.validateCompatiblePrimaryKeys(virtualTable.getPrimaryKey(),
                    physicalTable.getPrimaryKey());
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("incompatible table primary keys: "
                    + e.getMessage() + ", virtualTable=" + virtualTable + ", physicalTable=" + physicalTable);
            }
        }

        /**
         * Validate that the physical table's primary key and all of its secondary index's primary keys have valid
         * types.
         */
        void validatePhysicalTable(DynamoTableDescription physicalTableDescription) {
            String tableMsgPrefix = "physical table " + physicalTableDescription.getTableName();
            validatePhysicalPrimaryKey(physicalTableDescription.getPrimaryKey(), tableMsgPrefix);
            physicalTableDescription.getGsis().forEach(dynamoSecondaryIndex ->
                validatePhysicalPrimaryKey(dynamoSecondaryIndex.getPrimaryKey(), tableMsgPrefix
                    + "'s GSI " + dynamoSecondaryIndex.getIndexName()));
            physicalTableDescription.getLsis().forEach(dynamoSecondaryIndex ->
                validatePhysicalPrimaryKey(dynamoSecondaryIndex.getPrimaryKey(), tableMsgPrefix
                    + "'s LSI " + dynamoSecondaryIndex.getIndexName()));
        }

        private void validatePhysicalPrimaryKey(PrimaryKey primaryKey, String msgPrefix) {
            checkArgument(partitioningStrategy.isPhysicalPrimaryKeyValid(primaryKey),
                msgPrefix + " has invalid primary key: " + primaryKey);
        }
    }

}