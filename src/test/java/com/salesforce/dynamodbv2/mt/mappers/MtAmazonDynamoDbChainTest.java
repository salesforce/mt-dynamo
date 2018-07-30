/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * This tests that chaining works.  Note that this is a white-box test.  It inspects the calls to dynamo to see if
 * account, table, and index mapping has been applied.
 *
 * <p>Chain: amazonDynamoDbBySharedTable -> amazonDynamoDbByTable -> amazonDynamoDbLogger -> amazonDynamoDbByAccount
 *
 * @author msgroi
 */
class MtAmazonDynamoDbChainTest {

    @Test
    void test() throws Exception {
        // create context
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();

        // log message aggregator
        LogAggregator logAggregator = new LogAggregator();

        // get shared amazondynamodb
        AmazonDynamoDB amazonDynamoDb = MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDb();

        // create builders
        AmazonDynamoDB amazonDynamoDbByAccount = MtAmazonDynamoDbByAccount.accountMapperBuilder()
            .withAccountMapper(mtContext1 -> {
                if (mtContext1.getContext().equals("ctx1")) {
                    return new MockAmazonDynamoDb(mtContext, amazonDynamoDb);
                } else {
                    return new MockAmazonDynamoDb(mtContext, amazonDynamoDb);
                }
            })
            .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDbLogger = MtAmazonDynamoDbLogger.builder()
            .withAmazonDynamoDb(amazonDynamoDbByAccount)
            .withContext(mtContext)
            .withLogCallback(logAggregator)
            .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "getItem",
                "putItem", "query", "scan", "updateItem")).build();
        AmazonDynamoDB amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(amazonDynamoDbLogger)
            .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDbBySharedTable = SharedTableBuilder.builder()
            .withPrecreateTables(false)
            .withStreamsEnabled(false)
            .withAmazonDynamoDb(amazonDynamoDbByTable)
            .withContext(mtContext)
            .withTruncateOnDeleteTable(true).build();
        MtAmazonDynamoDbTestRunner testRunner = new MtAmazonDynamoDbTestRunner(mtContext,
                amazonDynamoDbBySharedTable,
                amazonDynamoDb,
                null,
                true);

        // setup
        testRunner.setup();
        logAggregator.messages.clear();

        // run
        testRunner.run();

        // log
        logAggregator.messages.forEach(message -> System.out.println("\"" + message + "\","));

        // assert
        final List<String> expectedLogMessages;
        try (Stream<String> stream = Files.lines(Paths.get(Resources
                .getResource("MtAmazonDynamoDbChainTest-expectedLogMessages.txt").toURI()))) {
            expectedLogMessages = stream.collect(Collectors.toList());
        }
        assertThat(logAggregator.messages, is(expectedLogMessages));

        // run
        testRunner.runWithoutLogging();

        // teardown
        testRunner.teardown();
    }

    private static class MockAmazonDynamoDb extends MtAmazonDynamoDbBase {

        MockAmazonDynamoDb(MtAmazonDynamoDbContextProvider mtContext, AmazonDynamoDB amazonDynamoDb) {
            super(mtContext, amazonDynamoDb);
        }

    }

    private static class LogAggregator implements Consumer<List<String>> {
        final List<String> messages = new ArrayList<>();

        @Override
        public void accept(List<String> messages) {
            this.messages.addAll(messages);
        }
    }

}