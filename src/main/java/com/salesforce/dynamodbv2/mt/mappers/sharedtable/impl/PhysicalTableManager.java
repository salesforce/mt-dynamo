package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static java.lang.String.format;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTableManager.class);

    private final AmazonDynamoDbAdminUtils dynamoDbAdminUtils;
    private final int pollIntervalSeconds;
    private final Map<String, DynamoTableDescription> physicalTableDescriptions;
    private final boolean canCreateTables;

    public PhysicalTableManager(AmazonDynamoDB amazonDynamoDb, int pollIntervalSeconds, boolean canCreateTables,
                                Collection<CreateTableRequest> tablesToCreateEagerly) {
        this.dynamoDbAdminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.physicalTableDescriptions = new ConcurrentHashMap<>();
        this.canCreateTables = canCreateTables;
        if (canCreateTables && tablesToCreateEagerly != null && !tablesToCreateEagerly.isEmpty()) {
            tablesToCreateEagerly.forEach(this::createTableIfNotExists);
        }
    }

    /**
     * Used by {@link TableMappingFactory} upon creation of a {@link TableMapping}, to ensure that the physical table
     * that a virtual table is mapped to exists and to retrieve the table description from remote Dynamo.
     * <p/>
     * When we are allowed to create tables, then this will create the physical table if it doesn't already exist;
     * otherwise, we assume that the physical table has already been created, and simply do a describe.
     */
    DynamoTableDescription ensurePhysicalTableExists(CreateTableRequest createTableRequest) {
        return canCreateTables
            ? createTableIfNotExists(createTableRequest)
            : describeTable(createTableRequest.getTableName());
    }

    DynamoTableDescription describeTable(String tableName) {
        return physicalTableDescriptions.computeIfAbsent(tableName, ignored ->
            new DynamoTableDescriptionImpl(dynamoDbAdminUtils.describeTable(tableName)));
    }

    private DynamoTableDescription createTableIfNotExists(CreateTableRequest physicalTable) {
        if (!canCreateTables) {
            throw new UnsupportedOperationException("Cannot create physical tables in this mt-dynamo client");
        }
        final String tableName = physicalTable.getTableName();
        return physicalTableDescriptions.computeIfAbsent(tableName, ignored ->
            new DynamoTableDescriptionImpl(dynamoDbAdminUtils.describeTableIfExists(tableName)
                .map(description -> {
                    LOG.info(format("Using existing physical table %s", tableName));
                    return description;
                }).orElseGet(() -> {
                    LOG.info(format("Creating physical table %s", physicalTable.getTableName()));
                    dynamoDbAdminUtils.createTableIfNotExists(physicalTable, pollIntervalSeconds);
                    return dynamoDbAdminUtils.describeTable(tableName);
                }))
        );
    }

}
