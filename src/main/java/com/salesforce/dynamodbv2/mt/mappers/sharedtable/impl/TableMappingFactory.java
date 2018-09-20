/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapper;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;

import java.util.Optional;

/**
 * Creates TableMapping objects that contain the state of given mapping of a virtual table to a physical table.  The
 * TableMapping also includes methods for retrieving the virtual and physical descriptions, and logic for mapping
 * of fields from virtual to physical and back.
 *
 * <p>This class is also responsible for triggering the creation of the physical tables appropriately.
 *
 * @author msgroi
 */
public class TableMappingFactory {

    private final AmazonDynamoDbAdminUtils dynamoDbAdminUtils;
    private final CreateTableRequestFactory createTableRequestFactory;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final DynamoSecondaryIndexMapper secondaryIndexMapper;
    private final String delimiter;
    private final AmazonDynamoDB amazonDynamoDb;
    private final int pollIntervalSeconds;

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequestFactory maps virtual to physical table instances
     * @param mtContext the multitenant context provider
     * @param secondaryIndexMapper maps virtual to physical indexes
     * @param delimiter the string for delimiting values in hash keys
     * @param amazonDynamoDb the underlying {@code AmazonDynamoDB} delegate
     * @param precreateTables a flag indicating whether to create physical tables eagerly at start time
     * @param pollIntervalSeconds the interval in seconds between attempts at checking the status of the table being
     *     created
     */
    public TableMappingFactory(CreateTableRequestFactory createTableRequestFactory,
                               MtAmazonDynamoDbContextProvider mtContext,
                               DynamoSecondaryIndexMapper secondaryIndexMapper,
                               String delimiter,
                               AmazonDynamoDB amazonDynamoDb,
                               boolean precreateTables,
                               int pollIntervalSeconds) {
        this.createTableRequestFactory = createTableRequestFactory;
        this.secondaryIndexMapper = secondaryIndexMapper;
        this.mtContext = mtContext;
        this.delimiter = delimiter;
        this.amazonDynamoDb = amazonDynamoDb;
        this.dynamoDbAdminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.pollIntervalSeconds = pollIntervalSeconds;
        if (precreateTables) {
            precreateTables(createTableRequestFactory);
        }
    }

    CreateTableRequestFactory getCreateTableRequestFactory() {
        return createTableRequestFactory;
    }

    private void precreateTables(CreateTableRequestFactory createTableRequestFactory) {
        createTableRequestFactory.getPhysicalTables().forEach(this::createTableIfNotExists);
    }

    /*
     * Creates the table mapping, creates the table if it does not exist, sets the physical table description
     * back onto the table mapping so it includes things that can only be determined after the physical
     * table is created, like the streamArn.
     */
    TableMapping getTableMapping(DynamoTableDescription virtualTableDescription) {
        TableMapping tableMapping = new TableMapping(virtualTableDescription,
            createTableRequestFactory,
            secondaryIndexMapper,
            mtContext,
            delimiter);
        tableMapping.setPhysicalTable(createTableIfNotExists(tableMapping.getPhysicalTable().getCreateTableRequest()));
        return tableMapping;
    }

    private DynamoTableDescriptionImpl createTableIfNotExists(CreateTableRequest physicalTable) {
        // does not exist, create
        if (!getTableDescription(physicalTable.getTableName()).isPresent()) {
            dynamoDbAdminUtils.createTableIfNotExists(physicalTable, pollIntervalSeconds);
        }
        return new DynamoTableDescriptionImpl(amazonDynamoDb.describeTable(physicalTable.getTableName()).getTable());
    }

    private Optional<TableDescription> getTableDescription(String tableName) {
        try {
            return Optional.of(amazonDynamoDb.describeTable(tableName).getTable());
        } catch (ResourceNotFoundException e) {
            return Optional.empty();
        } catch (IllegalStateException e) {
            throw new RuntimeException("Mt context available.  When chaining, you must either set the mt context "
                + "before building, or set precreateTables=false");
        }
    }

}