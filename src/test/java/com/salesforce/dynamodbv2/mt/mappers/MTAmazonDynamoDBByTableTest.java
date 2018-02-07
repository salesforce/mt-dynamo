/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author msgroi
 */
class MTAmazonDynamoDBByTableTest {

    private final boolean loggingEnabled = true;
    private final MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();
    private final AmazonDynamoDB localAmazonDynamoDB = MTAmazonDynamoDBTestRunner.getLocalAmazonDynamoDB();
    private final AmazonDynamoDB amazonDynamoDBLogger = MTAmazonDynamoDBLogger.builder()
            .withAmazonDynamoDB(localAmazonDynamoDB)
            .withContext(mtContext)
            .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                                               "putItem", "query", "scan", "updateItem")).build();
    private final AmazonDynamoDB amazonDynamoDB = loggingEnabled ? amazonDynamoDBLogger : localAmazonDynamoDB;

    @Test
    void test() {
        AmazonDynamoDB amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(amazonDynamoDB)
                .withContext(mtContext).build();
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                () -> amazonDynamoDBByTable,
                true).runAll();
    }

    @Test
    void prefixTablename() {
        /*
         * w/ tablePrefix
         */
        MTAmazonDynamoDBContextProvider mtContext = mock(MTAmazonDynamoDBContextProvider.class);
        MTAmazonDynamoDBByTable amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withDelimiter("-")
                .withTablePrefix("msgroi.")
                .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        assertEquals("msgroi.ctx1-originalTableFoo", amazonDynamoDBByTable.buildPrefixedTablename("originalTableFoo"));
        when(mtContext.getContext()).thenReturn("ctx2");
        assertEquals("msgroi.ctx2-originalTableBar", amazonDynamoDBByTable.buildPrefixedTablename("originalTableBar"));
        MTAmazonDynamoDBByTable amazonDynamoDBByTable2 = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withTablePrefix("msgroi-")
                .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx3");
        assertEquals("msgroi-ctx3.originalTableFooBar", amazonDynamoDBByTable2.buildPrefixedTablename("originalTableFooBar"));

        /*
         * w/out tablePrefix
         */
        amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withDelimiter("-")
                .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        assertEquals("ctx1-originalTableFoo", amazonDynamoDBByTable.buildPrefixedTablename("originalTableFoo"));
        when(mtContext.getContext()).thenReturn("ctx2");
        assertEquals("ctx2-originalTableBar", amazonDynamoDBByTable.buildPrefixedTablename("originalTableBar"));
        amazonDynamoDBByTable2 = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx3");
        assertEquals("ctx3.originalTableFooBar", amazonDynamoDBByTable2.buildPrefixedTablename("originalTableFooBar"));
    }

}