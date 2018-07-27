/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.StreamViewType.KEYS_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class MtAmazonDynamoDbByTableTest {

    private final boolean loggingEnabled = true;
    private final AmazonDynamoDB localAmazonDynamoDb = MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDb();

    @Test
    void test() {
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();
        AmazonDynamoDB amazonDynamoDbLogger = MtAmazonDynamoDbLogger.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withContext(mtContext)
            .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                "putItem", "query", "scan", "updateItem")).build();
        AmazonDynamoDB amazonDynamoDb = loggingEnabled ? amazonDynamoDbLogger : localAmazonDynamoDb;
        AmazonDynamoDB amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(mtContext).build();
        new MtAmazonDynamoDbTestRunner(
            mtContext,
            amazonDynamoDbByTable,
            amazonDynamoDb,
            null,
            true).runAll();
    }

    @Test
    void prefixTablename() {
        /*
         * w/ tablePrefix
         */
        MtAmazonDynamoDbContextProvider mtContext = mock(MtAmazonDynamoDbContextProvider.class);
        MtAmazonDynamoDbByTable amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withDelimiter("-")
            .withTablePrefix("msgroi.")
            .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        assertEquals("msgroi.ctx1-originalTableFoo",
                amazonDynamoDbByTable.buildPrefixedTablename("originalTableFoo"));
        when(mtContext.getContext()).thenReturn("ctx2");
        assertEquals("msgroi.ctx2-originalTableBar",
                amazonDynamoDbByTable.buildPrefixedTablename("originalTableBar"));
        MtAmazonDynamoDbByTable amazonDynamoDbByTable2 = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withTablePrefix("msgroi-")
            .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx3");
        assertEquals("msgroi-ctx3.originalTableFooBar",
                amazonDynamoDbByTable2.buildPrefixedTablename("originalTableFooBar"));

        /*
         * w/out tablePrefix
         */
        amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withDelimiter("-")
            .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        assertEquals("ctx1-originalTableFoo",
                amazonDynamoDbByTable.buildPrefixedTablename("originalTableFoo"));
        when(mtContext.getContext()).thenReturn("ctx2");
        assertEquals("ctx2-originalTableBar",
                amazonDynamoDbByTable.buildPrefixedTablename("originalTableBar"));
        amazonDynamoDbByTable2 = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx3");
        assertEquals("ctx3.originalTableFooBar",
                amazonDynamoDbByTable2.buildPrefixedTablename("originalTableFooBar"));
    }

    @Test
    void listStreams() {
        MtAmazonDynamoDbContextProvider mtContext = mock(MtAmazonDynamoDbContextProvider.class);
        MtAmazonDynamoDbByTable amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(localAmazonDynamoDb)
            .withContext(mtContext).build();
        when(mtContext.getContext()).thenReturn("ctx1");
        int count = 110;
        // create more than 100 tables with stream specs, since this exercises thelistStreams default maxsize
        for (int i = 1; i <= count; i++) {
            amazonDynamoDbByTable.createTable(new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withTableName("table" + i)
                .withStreamSpecification(new StreamSpecification()
                        .withStreamEnabled(true)
                        .withStreamViewType(KEYS_ONLY)));
        }
        assertEquals(count, amazonDynamoDbByTable.listStreams(null).size());
    }

}