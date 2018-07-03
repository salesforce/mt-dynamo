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
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * This tests that chaining works.  Note that this is a white-box test.  It inspects the calls to dynamo to see if
 * account, table, and index mapping has been applied.
 *
 * Chain: amazonDynamoDBBySharedTable -> amazonDynamoDBByTable -> amazonDynamoDBLogger -> amazonDynamoDBByAccount
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
        AmazonDynamoDB amazonDynamoDBBySharedTable = SharedTableBuilder.builder()
                .withPrecreateTables(false)
                .withAmazonDynamoDB(amazonDynamoDBByTable)
                .withContext(mtContext)
                .withTruncateOnDeleteTable(true).build();
        MTAmazonDynamoDBTestRunner testRunner = new MTAmazonDynamoDBTestRunner(mtContext, amazonDynamoDBBySharedTable, amazonDynamoDB, null, true);

        // setup
        testRunner.setup(); logAggregator.messages.clear();

        // run
        testRunner.run();

        // log
        logAggregator.messages.forEach(message -> System.out.println("\"" + message + "\","));

        // assert
        assertThat(logAggregator.messages, is(ImmutableList.of(
            "method=createTable(), table=ctx1.mt_sharedtablestatic_s_nolsi, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: gsi_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_rk,AttributeType: S}, {AttributeName: gsi_s_n_hk,AttributeType: S}, {AttributeName: gsi_s_n_rk,AttributeType: N}, {AttributeName: gsi_s_b_hk,AttributeType: S}, {AttributeName: gsi_s_b_rk,AttributeType: B}],TableName: ctx1.mt_sharedtablestatic_s_nolsi,KeySchema: [{AttributeName: hk,KeyType: HASH}],GlobalSecondaryIndexes: [{IndexName: gsi_s,KeySchema: [{AttributeName: gsi_s_hk,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_s,KeySchema: [{AttributeName: gsi_s_s_hk,KeyType: HASH}, {AttributeName: gsi_s_s_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_n,KeySchema: [{AttributeName: gsi_s_n_hk,KeyType: HASH}, {AttributeName: gsi_s_n_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_b,KeySchema: [{AttributeName: gsi_s_b_hk,KeyType: HASH}, {AttributeName: gsi_s_b_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},StreamSpecification: {StreamEnabled: true,StreamViewType: NEW_AND_OLD_IMAGES},}",
            "method=putItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, item={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}, someField={S: someValue1,}}",
            "method=createTable(), table=ctx2.mt_sharedtablestatic_s_nolsi, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: gsi_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_rk,AttributeType: S}, {AttributeName: gsi_s_n_hk,AttributeType: S}, {AttributeName: gsi_s_n_rk,AttributeType: N}, {AttributeName: gsi_s_b_hk,AttributeType: S}, {AttributeName: gsi_s_b_rk,AttributeType: B}],TableName: ctx2.mt_sharedtablestatic_s_nolsi,KeySchema: [{AttributeName: hk,KeyType: HASH}],GlobalSecondaryIndexes: [{IndexName: gsi_s,KeySchema: [{AttributeName: gsi_s_hk,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_s,KeySchema: [{AttributeName: gsi_s_s_hk,KeyType: HASH}, {AttributeName: gsi_s_s_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_n,KeySchema: [{AttributeName: gsi_s_n_hk,KeyType: HASH}, {AttributeName: gsi_s_n_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_b,KeySchema: [{AttributeName: gsi_s_b_hk,KeyType: HASH}, {AttributeName: gsi_s_b_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},StreamSpecification: {StreamEnabled: true,StreamViewType: NEW_AND_OLD_IMAGES},}",
            "method=putItem(), table=ctx2.mt_sharedtablestatic_s_nolsi, item={hk={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}, someField={S: someValue2,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_nolsi, keyConditionExpression=#name = :value, names={#name=hk}, values={:value={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_nolsi, keyConditionExpression=#field1 = :value1, names={#field1=hk}, values={:value1={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx2.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx2.mt_sharedtablestatic_s_nolsi, keyConditionExpression=#name = :value, names={#name=hk}, values={:value={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=query(), table=ctx2.mt_sharedtablestatic_s_nolsi, keyConditionExpression=#field1 = :value, names={#field1=hk}, values={:value={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx2.mt_sharedtablestatic_s_nolsi, filterExpression=#name = :value, names={#name=hk}, values={:value={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx2.mt_sharedtablestatic_s_nolsi, filterExpression=#field1 = :value1, names={#field1=hk}, values={:value1={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=scan(), table=ctx2.mt_sharedtablestatic_s_nolsi, filterExpression=#name = :value and begins_with(#___name___, :___value___), names={#___name___=hk, #name=someField}, values={:___value___={S: ctx2.MTAmazonDynamoDBTestRunner1.,}, :value={S: someValue2,}}",
            "method=scan(), table=ctx1.mt_sharedtablestatic_s_nolsi, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.MTAmazonDynamoDBTestRunner1.,}}",
            "method=scan(), table=ctx2.mt_sharedtablestatic_s_nolsi, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx2.MTAmazonDynamoDBTestRunner1.,}}",
            "method=updateItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, , attributeUpdates={someField={Value: {S: someValue1Updated,},}}, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=updateItem(), table=ctx2.mt_sharedtablestatic_s_nolsi, , attributeUpdates={someField={Value: {S: someValue2Updated,},}}, key={hk={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx2.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=updateItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, , updateExpression=set #name = :newValue, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}, conditionExpression=#name = :currentValue, names={#name=someField}, values={:currentValue={S: invalidValue,}, :newValue={S: someValue1UpdatedAgain,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=updateItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, , updateExpression=set #name = :newValue, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}, conditionExpression=#name = :currentValue, names={#name=someField}, values={:currentValue={S: someValue1Updated,}, :newValue={S: someValue1UpdatedAgain,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=putItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, item={hk={S: ctx1.MTAmazonDynamoDBTestRunner2.hashKeyValue,}, someField={S: someValueTable2,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner2.hashKeyValue,}}",
            "method=deleteItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner1.someValue1Updated,}}",
            "method=getItem(), table=ctx2.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx2.MTAmazonDynamoDBTestRunner1.hashKeyValue,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_nolsi, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner2.hashKeyValue,}}",
            "method=createTable(), table=ctx1.mt_sharedtablestatic_s_s, {AttributeDefinitions: [{AttributeName: hk,AttributeType: S}, {AttributeName: rk,AttributeType: S}, {AttributeName: gsi_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_hk,AttributeType: S}, {AttributeName: gsi_s_s_rk,AttributeType: S}, {AttributeName: gsi_s_n_hk,AttributeType: S}, {AttributeName: gsi_s_n_rk,AttributeType: N}, {AttributeName: gsi_s_b_hk,AttributeType: S}, {AttributeName: gsi_s_b_rk,AttributeType: B}, {AttributeName: lsi_s_s_rk,AttributeType: S}, {AttributeName: lsi_s_n_rk,AttributeType: N}, {AttributeName: lsi_s_b_rk,AttributeType: B}],TableName: ctx1.mt_sharedtablestatic_s_s,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: rk,KeyType: RANGE}],LocalSecondaryIndexes: [{IndexName: lsi_s_s,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: lsi_s_s_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,}}, {IndexName: lsi_s_n,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: lsi_s_n_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,}}, {IndexName: lsi_s_b,KeySchema: [{AttributeName: hk,KeyType: HASH}, {AttributeName: lsi_s_b_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,}}],GlobalSecondaryIndexes: [{IndexName: gsi_s,KeySchema: [{AttributeName: gsi_s_hk,KeyType: HASH}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_s,KeySchema: [{AttributeName: gsi_s_s_hk,KeyType: HASH}, {AttributeName: gsi_s_s_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_n,KeySchema: [{AttributeName: gsi_s_n_hk,KeyType: HASH}, {AttributeName: gsi_s_n_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}, {IndexName: gsi_s_b,KeySchema: [{AttributeName: gsi_s_b_hk,KeyType: HASH}, {AttributeName: gsi_s_b_rk,KeyType: RANGE}],Projection: {ProjectionType: ALL,},ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1}}],ProvisionedThroughput: {ReadCapacityUnits: 1,WriteCapacityUnits: 1},StreamSpecification: {StreamEnabled: true,StreamViewType: NEW_AND_OLD_IMAGES},}",
            "method=putItem(), table=ctx1.mt_sharedtablestatic_s_s, item={hk={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue3,}, rk={S: rangeKeyValue3a,}, someField={S: someValue3a,}}",
            "method=putItem(), table=ctx1.mt_sharedtablestatic_s_s, item={hk={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue3,}, rk={S: rangeKeyValue3b,}, someField={S: someValue3b,}}",
            "method=getItem(), table=ctx1.mt_sharedtablestatic_s_s, key={hk={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue3,}, rk={S: rangeKeyValue3a,}}",
            "method=scan(), table=ctx1.mt_sharedtablestatic_s_nolsi, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.MTAmazonDynamoDBTestRunner1.,}}",
            "method=deleteItem(), table=ctx1._tablemetadata, key={table={S: ctx1.MTAmazonDynamoDBTestRunner1,}}",
            "method=getItem(), table=ctx1._tablemetadata, key={table={S: ctx1.MTAmazonDynamoDBTestRunner1,}}",
            "method=getItem(), table=ctx1._tablemetadata, key={table={S: ctx1.MTAmazonDynamoDBTestRunner1,}}",
            "method=putItem(), table=ctx1._tablemetadata, item={data={S: {\"attributeDefinitions\":[{\"attributeName\":\"hashKeyField\",\"attributeType\":\"S\"}],\"tableName\":\"MTAmazonDynamoDBTestRunner1\",\"keySchema\":[{\"attributeName\":\"hashKeyField\",\"keyType\":\"HASH\"}],\"provisionedThroughput\":{\"readCapacityUnits\":1,\"writeCapacityUnits\":1}},}, table={S: ctx1.MTAmazonDynamoDBTestRunner1,}}",
            "method=getItem(), table=ctx1._tablemetadata, key={table={S: ctx1.MTAmazonDynamoDBTestRunner1,}}",
            "method=scan(), table=ctx1.mt_sharedtablestatic_s_nolsi, filterExpression=begins_with(#___name___, :___value___), names={#___name___=hk}, values={:___value___={S: ctx1.MTAmazonDynamoDBTestRunner1.,}}",
            "method=putItem(), table=ctx1.mt_sharedtablestatic_s_s, item={hk={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue,}, gsi_s_hk={S: ctx1.testgsi.indexFieldValue,}, rk={S: rangeKeyValue,}, lsi_s_s_rk={S: indexFieldValue,}}",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_s, keyConditionExpression=#name = :value AND #name2 = :value2, names={#name2=rk, #name=hk}, values={:value2={S: rangeKeyValue,}, :value={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue,}}",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_s, keyConditionExpression=#name = :value, filterExpression=#name2 = :value2, names={#name2=someField, #name=hk}, values={:value2={S: someValue3a,}, :value={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue3,}}",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_s, keyConditionExpression=#name = :value, names={#name=gsi_s_hk}, values={:value={S: ctx1.testgsi.indexFieldValue,}}, index=gsi_s",
            "method=query(), table=ctx1.mt_sharedtablestatic_s_s, keyConditionExpression=#name = :value and #name2 = :value2, names={#name2=lsi_s_s_rk, #name=hk}, values={:value2={S: indexFieldValue,}, :value={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue,}}, index=lsi_s_s",
            "method=scan(), table=ctx1.mt_sharedtablestatic_s_s, filterExpression=#name1 = :value1 AND #name2 = :value2, names={#name2=rk, #name1=hk}, values={:value2={S: rangeKeyValue,}, :value1={S: ctx1.MTAmazonDynamoDBTestRunner3.hashKeyValue,}}",
            "method=scan(), table=ctx1.mt_sharedtablestatic_s_s, filterExpression=#name = :value and begins_with(#___name___, :___value___), names={#___name___=hk, #name=gsi_s_hk}, values={:___value___={S: ctx1.MTAmazonDynamoDBTestRunner3.,}, :value={S: ctx1.testgsi.indexFieldValue,}}"
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