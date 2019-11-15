/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static java.lang.String.format;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTableManager.class);

    private final AmazonDynamoDbAdminUtils dynamoDbAdminUtils;
    private final int pollIntervalSeconds;
    private final Map<String, DynamoTableDescription> physicalTableDescriptions;

    public PhysicalTableManager(AmazonDynamoDB amazonDynamoDb, int pollIntervalSeconds) {
        this.dynamoDbAdminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.physicalTableDescriptions = new ConcurrentHashMap<>();
    }

    DynamoTableDescription describeTable(String tableName) {
        return physicalTableDescriptions.computeIfAbsent(tableName, ignored ->
            new DynamoTableDescriptionImpl(dynamoDbAdminUtils.describeTable(tableName)));
    }

    DynamoTableDescription createTableIfNotExists(CreateTableRequest physicalTable) {
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

    void deleteTableIfExists(String physicalTableName, int timeoutSeconds) {
        dynamoDbAdminUtils.deleteTableIfExists(physicalTableName, pollIntervalSeconds, timeoutSeconds);
        LOG.info(format("Deleted physical table %s", physicalTableName));
    }
}
