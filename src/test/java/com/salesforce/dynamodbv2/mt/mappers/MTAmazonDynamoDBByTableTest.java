/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import org.junit.jupiter.api.Test;

import static com.amazonaws.services.dynamodbv2.model.StreamViewType.KEYS_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author msgroi
 */
class MTAmazonDynamoDBByTableTest {

    @SuppressWarnings("FieldCanBeLocal")
    private final boolean loggingEnabled = true;
    private final AmazonDynamoDB localAmazonDynamoDB = MTAmazonDynamoDBTestRunner.getLocalAmazonDynamoDB();

    @Test
    void test() {
        MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();
        AmazonDynamoDB amazonDynamoDBLogger = MTAmazonDynamoDBLogger.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withContext(mtContext)
                .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                        "putItem", "query", "scan", "updateItem")).build();
        AmazonDynamoDB amazonDynamoDB = loggingEnabled ? amazonDynamoDBLogger : localAmazonDynamoDB;
        AmazonDynamoDB amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(amazonDynamoDB)
                .withContext(mtContext).build();
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                amazonDynamoDBByTable,
                amazonDynamoDB,
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

    @Test
    void listStreams() {
        MTAmazonDynamoDBContextProvider mtContext = mock(MTAmazonDynamoDBContextProvider.class);
        MTAmazonDynamoDBByTable amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(localAmazonDynamoDB)
                .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        int count = 110;
        for (int i = 1; i <= count; i++) { // create more than 100 tables with stream specs, since this exercises the listStreams default maxsize
            amazonDynamoDBByTable.createTable(new CreateTableRequest()
                    .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
                    .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withTableName("table" + i)
                    .withStreamSpecification(new StreamSpecification().withStreamEnabled(true).withStreamViewType(KEYS_ONLY)));
        }
        assertEquals(count, amazonDynamoDBByTable.listStreams(null).size());
    }

}