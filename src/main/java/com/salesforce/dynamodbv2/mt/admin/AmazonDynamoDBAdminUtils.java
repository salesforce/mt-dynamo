/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.admin;

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * @author msgroi
 */
public class AmazonDynamoDBAdminUtils {

    private static final int tableDDLOperationTimeoutSeconds = 600;
    private static final Logger log = LoggerFactory.getLogger(AmazonDynamoDBAdminUtils.class);
    private final AmazonDynamoDB amazonDynamoDB;

    public AmazonDynamoDBAdminUtils(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public void createTableIfNotExists(CreateTableRequest createTableRequest, int pollIntervalSeconds) {
        try {
            if (!tableExists(createTableRequest.getTableName())) {
                String tableName = createTableRequest.getTableName();
                amazonDynamoDB.createTable(createTableRequest);
                awaitTableActive(tableName, pollIntervalSeconds, tableDDLOperationTimeoutSeconds);
            } else {
                DynamoTableDescription existingTableDesc = new DynamoTableDescriptionImpl(describeTable(createTableRequest.getTableName()));
                DynamoTableDescription createTableRequestDesc = new DynamoTableDescriptionImpl(createTableRequest);
                checkArgument(existingTableDesc.equals(createTableRequestDesc),
                              "existing table does not match create table request, " +
                              "existing: " + existingTableDesc + ", createTableRequest=" + createTableRequestDesc);
            }
        } catch (TableInUseException e) {
            if (e.getStatus().toUpperCase().equals("CREATING")) {
                awaitTableActive(createTableRequest.getTableName(), pollIntervalSeconds, tableDDLOperationTimeoutSeconds);
            } else {
                throw new ResourceInUseException("table=" + e.getTableName() + " is in " + e.getStatus() + " status");
            }
        }
    }

    public void deleteTableIfNotExists(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        try {
            if (!tableExists(tableName)) {
                return;
            }
            else {
                amazonDynamoDB.deleteTable(new DeleteTableRequest().withTableName(tableName));
            }
        } catch (TableInUseException e) {
            if (!e.getStatus().toUpperCase().equals("DELETING")) {
                throw new ResourceInUseException("table=" + e.getTableName() + " being deleted has status=" + e.getStatus());
            }
        }
        log.info("awaiting " + pollIntervalSeconds + "s for table=" + tableName + " to delete ...");
        await().pollInSameThread()
                .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, SECONDS)))
                .atMost(timeoutSeconds, SECONDS)
                .until(() -> !tableExists(tableName));
    }

    @SuppressWarnings("all")
    private void awaitTableActive(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        log.info("awaiting " + timeoutSeconds + "s for table=" + tableName + " to become active ...");
        await().pollInSameThread()
                .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, SECONDS)))
                .atMost(timeoutSeconds, SECONDS)
                .until(() -> tableActive(tableName));
    }

    @SuppressWarnings("all")
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
        return amazonDynamoDB.describeTable(tableName).getTable();
    }



}