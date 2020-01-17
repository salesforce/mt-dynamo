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
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import java.time.Duration;
import java.util.Optional;
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
     * @param createTableRequest  the description of the table to be created
     * @param pollIntervalSeconds the interval in seconds between attempts at checking the status of the table being
     *                            created
     */
    public void createTableIfNotExists(CreateTableRequest createTableRequest, int pollIntervalSeconds) {

        try {
            if (!tableExists(createTableRequest.getTableName(), TableStatus.ACTIVE)) {
                String tableName = createTableRequest.getTableName();
                amazonDynamoDb.createTable(createTableRequest);
                awaitTableActive(tableName, pollIntervalSeconds
                );
            } else {
                DynamoTableDescription existingTableDesc = new DynamoTableDescriptionImpl(describeTable(
                    createTableRequest.getTableName()));
                DynamoTableDescription createTableRequestDesc = new DynamoTableDescriptionImpl(createTableRequest);
                checkArgument(existingTableDesc.equals(createTableRequestDesc),
                    "existing table does not match create table request, "
                        + "existing: " + existingTableDesc + ", createTableRequest=" + createTableRequestDesc);
            }
        } catch (TableInUseException e) {
            if (TableStatus.CREATING.equals(e.getStatus())) {
                awaitTableActive(createTableRequest.getTableName(),
                    pollIntervalSeconds
                );
            } else {
                throw new ResourceInUseException("table=" + e.getTableName() + " is in " + e.getStatus() + " status");
            }
        }
    }

    /**
     * TODO: write Javadoc.
     *
     * @param tableName           the name of the table to be deleted
     * @param pollIntervalSeconds the interval in seconds between attempts at checking the status of the table being
     *                            created
     * @param timeoutSeconds      the time in seconds to wait for the table to no longer exist
     */
    public void deleteTableIfExists(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        try {
            if (!tableExists(tableName, TableStatus.DELETING)) {
                return;
            } else {
                amazonDynamoDb.deleteTable(new DeleteTableRequest().withTableName(tableName));
            }
        } catch (TableInUseException e) {
            if (!TableStatus.DELETING.equals(e.getStatus())) {
                throw new ResourceInUseException(
                    "table=" + e.getTableName() + " being deleted has status=" + e.getStatus());
            }
        }
        log.info("awaiting " + pollIntervalSeconds + "s for table=" + tableName + " to delete ...");
        await().pollInSameThread()
            .pollInterval(new FixedPollInterval(Duration.ofSeconds(pollIntervalSeconds)))
            .atMost(timeoutSeconds, SECONDS)
            .until(() -> !tableExists(tableName, TableStatus.DELETING));
    }

    private void awaitTableActive(String tableName, int pollIntervalSeconds) {
        int timeoutSeconds = TABLE_DDL_OPERATION_TIMEOUT_SECONDS;
        log.info("awaiting " + timeoutSeconds + "s for table=" + tableName + " to become active ...");
        await().pollInSameThread()
            .pollInterval(new FixedPollInterval(Duration.ofSeconds(pollIntervalSeconds)))
            .atMost(timeoutSeconds, SECONDS)
            .until(() -> tableActive(tableName));
    }

    private boolean tableExists(String tableName, TableStatus expectedTableStatus) throws TableInUseException {
        try {
            getTableStatus(tableName, expectedTableStatus);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private TableStatus getTableStatus(String tableName, TableStatus expectedTableStatus) throws TableInUseException {
        try {
            final TableStatus currentTableStatus = TableStatus.fromValue(describeTable(tableName).getTableStatus());
            log.debug("table=" + tableName + " is " + currentTableStatus);

            switch (currentTableStatus) {
                case ACTIVE:
                    break;
                case CREATING:
                case UPDATING:
                case DELETING:
                    if (!currentTableStatus.equals(expectedTableStatus)) {
                        throw new TableInUseException(tableName, currentTableStatus);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported TableStatus " + currentTableStatus);
            }
            return currentTableStatus;
        } catch (ResourceNotFoundException e) {
            log.info("table=" + tableName + " does not exist");
            throw e;
        }
    }

    private static class TableInUseException extends Exception {

        private final String tableName;
        private final TableStatus status;

        TableInUseException(String tableName, TableStatus status) {
            this.tableName = tableName;
            this.status = status;
        }

        String getTableName() {
            return tableName;
        }

        TableStatus getStatus() {
            return status;
        }
    }

    private boolean tableActive(String tableName) throws TableInUseException {
        try {
            return TableStatus.ACTIVE.equals(getTableStatus(tableName, TableStatus.CREATING));
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    public TableDescription describeTable(String tableName) {
        return amazonDynamoDb.describeTable(tableName).getTable();
    }

    public Optional<TableDescription> describeTableIfExists(String tableName) {
        try {
            return Optional.of(describeTable(tableName));
        } catch (ResourceNotFoundException e) {
            return Optional.empty();
        }
    }
}