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
import com.salesforce.dynamodbv2.mt.repo.MTDynamoDBTableDescriptionRepo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * This tests that chaining works.  Note that this is a whitebox test.  It inspects the calls to dynamo to see if
 * account, table, and index mapping has been applied.
 *
 * Chain: amazonDynamoDBByIndex -> amazonDynamoDBByTable -> amazonDynamoDBLogger -> amazonDynamoDBByAccount
 *
 * @author msgroi
 */
class MTAmazonDynamoDBChainTest {

    @Test
    void test() {
        // create context
        MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();

        // log message aggregator
        LogAggregator logAggregator = new LogAggregator();

        // get shared amazondynamodb
        AmazonDynamoDB amazonDynamoDB = MTAmazonDynamoDBTestRunner.getLocalAmazonDynamoDB();

        // create builders
        AmazonDynamoDB amazonDynamoDBByAccount = MTAmazonDynamoDBByAccount.accountMapperBuilder()
                .withAccountMapper(mtContext1 -> {
                    if (mtContext1.getContext().equals("ctx1")) {
                        return new MockAmazonDynamoDB2(mtContext, amazonDynamoDB);
                    } else {
                        return new MockAmazonDynamoDB2(mtContext, amazonDynamoDB);
                    }
                })
                .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDBLogger = MTAmazonDynamoDBLogger.builder()
                .withAmazonDynamoDB(amazonDynamoDBByAccount)
                .withContext(mtContext)
                .withLogCallback(logAggregator)
                .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "getItem",
                                                   "putItem", "query", "scan", "updateItem")).build();
        AmazonDynamoDB amazonDynamoDBByTable = MTAmazonDynamoDBByTable.builder()
                .withAmazonDynamoDB(amazonDynamoDBLogger)
                .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDBByIndex = MTAmazonDynamoDBByIndex.builder()
                .withAmazonDynamoDB(amazonDynamoDBByTable)
                .withTableDescriptionRepo(new MTDynamoDBTableDescriptionRepo(amazonDynamoDB, mtContext,0))
                .withContext(mtContext)
                .withPollIntervalSeconds(0)
                .withTruncateOnDeleteTable(true).build();
        MTAmazonDynamoDBTestRunner testRunner = new MTAmazonDynamoDBTestRunner(mtContext, () -> amazonDynamoDBByIndex, true);

        // setup
        testRunner.setup(); logAggregator.messages.clear();

        // run
        testRunner.run();

        // log
        logAggregator.messages.forEach(message -> System.out.println("\"" + message + "\","));

        // assert
        assertThat(logAggregator.messages, is(ImmutableList.of(
            "method=createTable(), table=ctx1._DATA_HK_S, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: gsi_s,AttributeType: S}],TableName: ctx1._DATA_HK_S,KeySchema: [{AttributeName: hk,KeyType: HASH}],GlobalSecondaryIndexes: [{IndexName: gsi,KeySchema: [{AttributeName: gsi_s,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner0.,}}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner0.,}}",
            "method=putItem(), table=ctx1._DATA_HK_S, item={hashKeyField={S: hashKeyValue,}, someField={S: someValue1,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=createTable(), table=ctx2._DATA_HK_S, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: gsi_s,AttributeType: S}],TableName: ctx2._DATA_HK_S,KeySchema: [{AttributeName: hk,KeyType: HASH}],GlobalSecondaryIndexes: [{IndexName: gsi,KeySchema: [{AttributeName: gsi_s,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},}",
            "method=putItem(), table=ctx2._DATA_HK_S, item={hashKeyField={S: hashKeyValue,}, someField={S: someValue2,}, hk={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx1._DATA_HK_S, filterExpression=#name = :value, names={#name=hk}, values={:value={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx2._DATA_HK_S, key={hk={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx2._DATA_HK_S, filterExpression=#name = :value, names={#name=hk}, values={:value={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx2._DATA_HK_S, filterExpression=hk = :value, names=null, values={:value={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx2._DATA_HK_S, filterExpression=#name = :value, names={#name=hk}, values={:value={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx2._DATA_HK_S, filterExpression=#name = :value and begins_with(#___name___, :___value___), names={#___name___=hk, #name=someField}, values={:___value___={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.,}, :value={S: someValue2,}}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.,}}",
            "method=scan(), table=ctx2._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.,}}",
            "method=updateItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=updateItem(), table=ctx2._DATA_HK_S, key={hk={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx2._DATA_HK_S, key={hk={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=putItem(), table=ctx1._DATA_HK_S, item={hashKeyField={S: hashKeyValue,}, someField={S: someValueTable2,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner2.hashKeyValue,}}",
            "method=getItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner2.hashKeyValue,}}",
            "method=deleteItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.someValue1Updated,}}",
            "method=getItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.someValue1Updated,}}",
            "method=getItem(), table=ctx2._DATA_HK_S, key={hk={S: ctx2.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner2.hashKeyValue,}}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.,}}",
            "method=createTable(), table=ctx1._DATA_HK_S_RK_S, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: rk,AttributeType: S}, {AttributeName: gsi_s,AttributeType: S}, {AttributeName: lsi_s,AttributeType: S}],TableName: ctx1._DATA_HK_S_RK_S,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: rk,KeyType: RANGE}],LocalSecondaryIndexes: [{IndexName: lsi,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: lsi_s,KeyType: RANGE}],Projection: {ProjectionType: ALL,}}],GlobalSecondaryIndexes: [{IndexName: gsi,KeySchema: [{AttributeName: gsi_s,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},}",
            "method=putItem(), table=ctx1._DATA_HK_S_RK_S, item={hashKeyField={S: hashKeyValue3,}, rk={S: rangeKeyValue3a,}, someField={S: someValue3a,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.hashKeyValue3,}, rangeKeyField={S: rangeKeyValue3a,}}",
            "method=putItem(), table=ctx1._DATA_HK_S_RK_S, item={hashKeyField={S: hashKeyValue3,}, rk={S: rangeKeyValue3b,}, someField={S: someValue3b,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.hashKeyValue3,}, rangeKeyField={S: rangeKeyValue3b,}}",
            "method=getItem(), table=ctx1._DATA_HK_S_RK_S, key={rk={S: rangeKeyValue3a,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.hashKeyValue3,}}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.,}}",
            "method=deleteItem(), table=ctx1._DATA_HK_S, key={hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx1._DATA_HK_S, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner1.,}}",
            "method=putItem(), table=ctx1._DATA_HK_S_RK_S, item={gsi_s={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.indexFieldValue,}, lsi_s={S: indexFieldValue,}, hk={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.hashKeyValue,}, rangeKeyField={S: rangeKeyValue,}, hashKeyField={S: hashKeyValue,}, rk={S: rangeKeyValue,}, indexField={S: indexFieldValue,}}",
            "method=query(), table=ctx1._DATA_HK_S_RK_S, filterExpression=#name = :value, names={#name=gsi_s}, values={:value={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.indexFieldValue,}}, index=gsi",
            "method=query(), table=ctx1._DATA_HK_S_RK_S, filterExpression=#name = :value and #name2 = :value2, names={#name2=lsi_s, #name=hk}, values={:value2={S: indexFieldValue,}, :value={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.hashKeyValue,}}, index=lsi",
            "method=scan(), table=ctx1._DATA_HK_S_RK_S, filterExpression=#name = :value and begins_with(#___name___, :___value___), names={#___name___=hk, #name=indexField}, values={:___value___={S: ctx1.oktodelete-MTAmazonDynamoDBTestRunner3.,}, :value={S: indexFieldValue,}}"
        )));

        // teardown
        testRunner.teardown();
    }

    private static class MockAmazonDynamoDB2 extends MTAmazonDynamoDBBase {

        MockAmazonDynamoDB2(MTAmazonDynamoDBContextProvider mtContext, AmazonDynamoDB amazonDynamoDB) {
            super(mtContext, amazonDynamoDB);
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