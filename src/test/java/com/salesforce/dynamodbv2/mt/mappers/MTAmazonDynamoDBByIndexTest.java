/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import com.salesforce.dynamodbv2.mt.repo.MTDynamoDBTableDescriptionRepo;
import org.junit.jupiter.api.Test;

/**
 * @author msgroi
 */
class MTAmazonDynamoDBByIndexTest {

    private final boolean loggingEnabled = true;
    private final MTAmazonDynamoDBContextProviderImpl mtContext = new MTAmazonDynamoDBContextProviderImpl();
    private final AmazonDynamoDB localAmazonDynamoDB = MTAmazonDynamoDBTestRunner.getLocalAmazonDynamoDB();
    private final AmazonDynamoDB amazonDynamoDBLogger = MTAmazonDynamoDBLogger.builder()
            .withAmazonDynamoDB(localAmazonDynamoDB)
            .withContext(mtContext)
            .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                                               "putItem", "query", "scan", "updateItem")).build();
    private final AmazonDynamoDB amazonDynamoDB = loggingEnabled ? amazonDynamoDBLogger : localAmazonDynamoDB;

    @Test
    void test() {
        MTAmazonDynamoDBByIndex mtAmazonDynamoDBByIndex = MTAmazonDynamoDBByIndex.builder()
                .withAmazonDynamoDB(amazonDynamoDB)
                .withContext(mtContext)
                .withPollIntervalSeconds(0)
                .withTableDescriptionRepo(new MTDynamoDBTableDescriptionRepo(amazonDynamoDB, mtContext, 0))
                .withTruncateOnDeleteTable(true)
                .build();
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                () -> mtAmazonDynamoDBByIndex,
                true).runAll();
    }

}