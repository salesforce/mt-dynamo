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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.annotations.VisibleForTesting;
import org.awaitility.Duration;
import org.awaitility.pollinterval.FixedPollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * @author msgroi
 */
public class AmazonDynamoDBAdminUtils {

    private static final Logger log = LoggerFactory.getLogger(AmazonDynamoDBAdminUtils.class);
    private final AmazonDynamoDB amazonDynamoDB;

    public AmazonDynamoDBAdminUtils(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public void createTableIfNotExists(CreateTableRequest createTableRequest, int pollIntervalSeconds) {
        if (!tableExists(createTableRequest.getTableName())) {
            String tableName = createTableRequest.getTableName();
            amazonDynamoDB.createTable(createTableRequest);
            awaitTableActive(tableName, pollIntervalSeconds, 90);
        }
    }

    public void deleteTableIfNotExists(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        if (tableExists(tableName)) {
            amazonDynamoDB.deleteTable(new DeleteTableRequest().withTableName(tableName));
            log.info("awaiting table=" + tableName + " to delete ...");
            await().pollInSameThread()
                    .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, TimeUnit.SECONDS)))
                    .atMost(timeoutSeconds, SECONDS)
                    .until(() -> !tableExists(tableName));
        }
    }

    @VisibleForTesting
    public void awaitTableActive(String tableName, int pollIntervalSeconds, int timeoutSeconds) {
        log.info("awaiting table=" + tableName + " to become active ...");
        // {TableNames: [ctx1.oktodelete.msgroi-ltm.MTAmazonDynamoDBTestRunner1],}
        await().pollInSameThread()
                .pollInterval(new FixedPollInterval(new Duration(pollIntervalSeconds, TimeUnit.SECONDS)))
                .atMost(timeoutSeconds, SECONDS)
                .until(() -> tableActive(tableName));
    }

    private boolean tableExists(String tableName) {
        try {
            getTableStatus(tableName);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private String getTableStatus(String tableName) {
        try {
            String status = describeTable(tableName).getTableStatus();
            log.info("table=" + tableName + " is " + status);
            return status;
        } catch (ResourceNotFoundException e) {
            log.info("table=" + tableName + " does not exist");
            throw e;
        }
    }

    private boolean tableActive(String tableName) {
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