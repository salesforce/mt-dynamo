/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.admin;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import org.awaitility.Duration;
import org.awaitility.pollinterval.FixedPollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class AmazonDynamoDbAdminUtils {

    private static final int TABLE_DDL_OPERATION_TIMEOUT_SECONDS = 600;
    private static final Logger log = LoggerFactory.getLogger(AmazonDynamoDbAdminUtils.class);
    private final AmazonDynamoDB amazonDynamoDb;

    public AmazonDynamoDbAdminUtils(AmazonDynamoDB amazonDynamoDb) {
        this.amazonDynamoDb = amazonDynamoDb;
    }

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequest the description of the table to be created
     * @param pollIntervalSeconds the interval in seconds between attempts at checking the status of the table being
     *     created
     */
    public void createTableIfNotExists(CreateTableRequest createTableRequest, int pollIntervalSeconds) {
        try {
            if (!tableExists(createTableRequest.getTableName())) {
                String tableName = createTableRequest.getTableName();
                amazonDynamoDb.createTable(createTableRequest);
                awaitTableActive(tableName, pollIntervalSeconds, TABLE_DDL_OPERATION_TIMEOUT_SECONDS);
            } else {
                DynamoTableDescription existingTableDesc = new DynamoTableDescriptionImpl(describeTable(
                    createTableRequest.getTableName()));
                DynamoTableDescription createTableRequestDesc = new DynamoTableDescriptionImpl(createTableRequest);
                checkArgument(existingTableDesc.equals(createTableRequestDesc),
                    "existing table does not match create table request, "
                        + "existing: " + existingTableDesc + ", createTableRequest=" + createTableRequestDesc);
            }
        } catch (TableInUseException e) {
            if (e.getStatus().toUpperCase().equals("CREATING")) {
                awaitTableActive(createTableRequest.getTableName(),
                                 pollIntervalSeconds,
                        TABLE_DDL_OPERATION_TIMEOUT_SECONDS);
            } else {
                throw new ResourceInUseException("table=" + e.getTableName() + " is in " + e.getStatus() + " status");
            }
        }
    }

    /**
     * TODO: write Javadoc.
     *
     * @param tableName the name of the table to be deleted
     * @param pollIntervalSeconds the interval in seconds between attempts at checking the status of the table being
     *     created
     * @param timeoutSeconds the time in seconds to wait for the table to no longer exist
     */
    public void deleteTableIfExists(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        try {
            if (!tableExists(tableName)) {
                return;
            } else {
                amazonDynamoDb.deleteTable(new DeleteTableRequest().withTableName(tableName));
            }
        } catch (TableInUseException e) {
            if (!e.getStatus().toUpperCase().equals("DELETING")) {
                throw new ResourceInUseException(
                    "table=" + e.getTableName() + " being deleted has status=" + e.getStatus());
            }
        }
        log.info("awaiting " + pollIntervalSeconds + "s for table=" + tableName + " to delete ...");
        await().pollInSameThread()
            .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, SECONDS)))
            .atMost(timeoutSeconds, SECONDS)
            .until(() -> !tableExists(tableName));
    }

    private void awaitTableActive(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        log.info("awaiting " + timeoutSeconds + "s for table=" + tableName + " to become active ...");
        await().pollInSameThread()
            .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, SECONDS)))
            .atMost(timeoutSeconds, SECONDS)
            .until(() -> tableActive(tableName));
    }

    private boolean tableExists(String tableName) throws TableInUseException {
        try {
            getTableStatus(tableName);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private String getTableStatus(String tableName) throws TableInUseException {
        try {
            String status = describeTable(tableName).getTableStatus();
            log.info("table=" + tableName + " is " + status);
            if (status.toLowerCase().equals("ing")) {
                throw new TableInUseException(tableName, status);
            }
            return status;
        } catch (ResourceNotFoundException e) {
            log.info("table=" + tableName + " does not exist");
            throw e;
        }
    }

    private static class TableInUseException extends Exception {
        private final String tableName;
        private final String status;

        TableInUseException(String tableName, String status) {
            this.tableName = tableName;
            this.status = status;
        }

        public String getTableName() {
            return tableName;
        }

        String getStatus() {
            return status;
        }
    }

    private boolean tableActive(String tableName) throws TableInUseException {
        try {
            return getTableStatus(tableName).equals("ACTIVE");
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private TableDescription describeTable(String tableName) {
        return amazonDynamoDb.describeTable(tableName).getTable();
    }


}