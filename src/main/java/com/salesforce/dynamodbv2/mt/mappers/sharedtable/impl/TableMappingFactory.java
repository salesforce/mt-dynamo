/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperTrackingAssigned;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    private final AmazonDynamoDbAdminUtils dynamoDbAdminUtils;
    private final CreateTableRequestFactory createTableRequestFactory;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final AmazonDynamoDB amazonDynamoDb;
    private final TablePartitioningStrategy partitioningStrategy;
    private final VirtualTableCreationValidator virtualTableCreationValidator;
    private final int pollIntervalSeconds;
    private final Map<String, DynamoTableDescription> physicalTableDescriptions;

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequestFactory maps virtual to physical table instances
     * @param mtContext                 the multitenant context provider
     * @param amazonDynamoDb            the underlying {@code AmazonDynamoDB} delegate
     * @param createTablesEagerly       a flag indicating whether to create physical tables eagerly at start time
     * @param pollIntervalSeconds       the interval in seconds between attempts at checking the status of the table
     *                                  being created
     */
    public TableMappingFactory(CreateTableRequestFactory createTableRequestFactory,
                               MtAmazonDynamoDbContextProvider mtContext,
                               AmazonDynamoDB amazonDynamoDb,
                               TablePartitioningStrategy partitioningStrategy,
                               boolean createTablesEagerly,
                               int pollIntervalSeconds) {
        this.createTableRequestFactory = createTableRequestFactory;
        this.mtContext = mtContext;
        this.amazonDynamoDb = amazonDynamoDb;
        this.dynamoDbAdminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.partitioningStrategy = partitioningStrategy;
        this.virtualTableCreationValidator = new VirtualTableCreationValidator(partitioningStrategy);
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.physicalTableDescriptions = new ConcurrentHashMap<>();
        if (createTablesEagerly) {
            createTablesEagerly(createTableRequestFactory);
        }
    }

    CreateTableRequestFactory getCreateTableRequestFactory() {
        return createTableRequestFactory;
    }

    private void createTablesEagerly(CreateTableRequestFactory createTableRequestFactory) {
        createTableRequestFactory.getPhysicalTables().forEach(this::createTableIfNotExists);
    }

    void validateCreateVirtualTableRequest(CreateTableRequest createVirtualTableRequest) {
        DynamoTableDescription virtualTable = new DynamoTableDescriptionImpl(createVirtualTableRequest);
        DynamoTableDescription physicalTable = lookupPhysicalTable(virtualTable);

        // validate physical table key types
        virtualTableCreationValidator.validatePhysicalTable(physicalTable);

        // validate primary key types are compatible
        virtualTableCreationValidator.validateCompatiblePrimaryKeys(virtualTable, physicalTable);

        // validate secondary indexes
        virtualTableCreationValidator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable);
    }

    /**
     * Calls the provided CreateTableRequestFactory passing in the virtual table description and returns the
     * corresponding physical table.  Throws a ResourceNotFoundException if the implementation returns null.
     */
    private DynamoTableDescription lookupPhysicalTable(DynamoTableDescription virtualTable) {
        return new DynamoTableDescriptionImpl(
            createTableRequestFactory.getCreateTableRequest(virtualTable).orElseThrow(() ->
                new ResourceNotFoundException("table " + virtualTable.getTableName() + " is not a supported table")));
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

    /**
     * Creates the table mapping, creates the table if it does not exist, sets the physical table description
     * back onto the table mapping so it includes things that can only be determined after the physical
     * table is created, like the streamArn.
     */
    TableMapping getTableMapping(DynamoTableDescription virtualTable) {
        DynamoTableDescription physicalTable = lookupPhysicalTable(virtualTable);
        physicalTable = createTableIfNotExists(physicalTable.getCreateTableRequest());
        Map<DynamoSecondaryIndex, DynamoSecondaryIndex> secondaryIndexMap =
            virtualTableCreationValidator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable);
        TableMapping tableMapping = partitioningStrategy.createTableMapping(virtualTable, physicalTable,
            secondaryIndexMap::get, mtContext);
        LOG.debug("created virtual to physical table mapping: " + tableMapping.toString());
        return tableMapping;
    }

    private DynamoTableDescription createTableIfNotExists(CreateTableRequest physicalTable) {
        // does not exist, create
        final String tableName = physicalTable.getTableName();
        return physicalTableDescriptions.computeIfAbsent(tableName, ignored ->
            new DynamoTableDescriptionImpl(getTableDescription(tableName)
                .map(description -> {
                    LOG.info(format("using existing physical table %s", tableName));
                    return description;
                }).orElseGet(() -> {
                    LOG.info(format("creating physical table %s", physicalTable.getTableName()));
                    dynamoDbAdminUtils.createTableIfNotExists(physicalTable, pollIntervalSeconds);
                    return amazonDynamoDb.describeTable(tableName).getTable();
                }))
        );
    }

    private Optional<TableDescription> getTableDescription(String tableName) {
        try {
            return Optional.of(amazonDynamoDb.describeTable(tableName).getTable());
        } catch (ResourceNotFoundException e) {
            return Optional.empty();
        } catch (IllegalStateException e) {
            throw new RuntimeException("Mt context available.  When chaining, you must either set the mt context "
                + "before building, or set createTablesEagerly=false");
        }
    }

}