/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests that chaining works.  Note that this is a white-box test.  It inspects the calls to dynamo to see if
 * account, table, and index mapping has been applied.
 *
 * <p>Chain: amazonDynamoDbBySharedTable -> amazonDynamoDbByTable -> amazonDynamoDbLogger -> amazonDynamoDbByAccount
 *
 * @author msgroi
 */
class MtAmazonDynamoDbChainTest {

    private static final Logger LOG = LoggerFactory.getLogger(MtAmazonDynamoDbChainTest.class);
    private static final String GOLD_FILE = "MtAmazonDynamoDbChainTest-expectedLogMessages.txt";
    private static final ScalarAttributeType HASH_KEY_ATTR_TYPE = S;
    private static final int TIMEOUT_SECONDS = 600;
    private static final boolean IS_LOCAL_DYNAMO = true;

    @Test
    void test() throws Exception {
        // create context
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();

        // log message aggregator
        LogAggregator logAggregator = new LogAggregator();

        // get shared amazondynamodb
        AmazonDynamoDB rootAmazonDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

        // create builders
        AmazonDynamoDB amazonDynamoDbByAccount = MtAmazonDynamoDbByAccount.accountMapperBuilder()
            .withAccountMapper(mtContext1 -> {
                if (mtContext1.getContext().equals("ctx1")) {
                    return new MockAmazonDynamoDb(mtContext, rootAmazonDynamoDb);
                } else {
                    return new MockAmazonDynamoDb(mtContext, rootAmazonDynamoDb);
                }
            })
            .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDbLogger = MtAmazonDynamoDbLogger.builder()
            .withAmazonDynamoDb(amazonDynamoDbByAccount)
            .withContext(mtContext)
            .withLogCallback(logAggregator)
            .withMethodsToLog(ImmutableList.of("batchGetItem", "createTable", "deleteItem", "deleteTable", "getItem",
                "putItem", "query", "scan", "updateItem")).build();
        AmazonDynamoDB amazonDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(amazonDynamoDbLogger)
            .withContext(mtContext).build();
        AmazonDynamoDB amazonDynamoDbBySharedTable = SharedTableBuilder.builder()
            .withStreamsEnabled(false)
            .withPrecreateTables(false)
            .withAmazonDynamoDb(amazonDynamoDbByTable)
            .withContext(mtContext)
            .withTruncateOnDeleteTable(true).build();

        // run
        run(mtContext,
            amazonDynamoDbBySharedTable,
            logAggregator);

        // assert
        final List<String> expectedLogMessages;
        try (Stream<String> stream = Files.lines(Paths.get(Resources
                .getResource(GOLD_FILE).toURI()))) {
            expectedLogMessages = stream.collect(Collectors.toList());
        }
        if (!is(expectedLogMessages).matches(logAggregator.messages)) {
            // log
            logAggregator.messages.forEach(message -> System.out.println("\"" + message + "\","));
            fail("Expected output does not match gold file.  To troubleshoot, diff the above"
                 + " console output against " + GOLD_FILE);
        }

        // teardown
        teardown(rootAmazonDynamoDb);
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

    private void run(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        LogAggregator logAggregator) {
        // build create table statements
        String tableName1 = buildTableName(1);
        String tableName2 = buildTableName(2);
        String tableName3 = buildTableName(3);
        CreateTableRequest createTableRequest1 = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, HASH_KEY_ATTR_TYPE))
            .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withTableName(tableName1);
        CreateTableRequest createTableRequest2 = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, HASH_KEY_ATTR_TYPE))
            .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withTableName(tableName2);
        CreateTableRequest createTableRequest3 = new CreateTableRequest()
            .withTableName(tableName3)
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, HASH_KEY_ATTR_TYPE),
                new AttributeDefinition(RANGE_KEY_FIELD, S),
                new AttributeDefinition(INDEX_FIELD, S))
            .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
            .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                    new KeySchemaElement(INDEX_FIELD, KeyType.RANGE))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
        List<Map<String, CreateTableRequest>> ctxTablePairs = new ArrayList<>(ImmutableList.of(
            ImmutableMap.of("ctx1", createTableRequest1),
            ImmutableMap.of("ctx2", createTableRequest1),
            ImmutableMap.of("ctx1", createTableRequest2),
            ImmutableMap.of("ctx1", createTableRequest3),
            ImmutableMap.of("ctx3", createTableRequest1),
            ImmutableMap.of("ctx4", createTableRequest1)
        ));

        // create tables
        for (Map<String, CreateTableRequest> ctxTablePair : ctxTablePairs) {
            Entry<String, CreateTableRequest> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
            recreateTable(mtContext, amazonDynamoDb, ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
        }

        // clear messages
        logAggregator.messages.clear();

        // log start
        String testDescription = amazonDynamoDb + " with hashkey type=" + HASH_KEY_ATTR_TYPE;
        LOG.info("START test " + testDescription);

        // table with hk only

        // describe table in ctx1
        mtContext.setContext("ctx1");
        assertEquals(tableName1, amazonDynamoDb.describeTable(tableName1).getTable().getTableName());

        // describe table in ctx2
        mtContext.setContext("ctx2");
        assertEquals(tableName1, amazonDynamoDb.describeTable(tableName1).getTable().getTableName());

        // put item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item =
                ItemBuilder.builder(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)
                        .someField(S, "someValue1")
                        .build();
        Map<String, AttributeValue> originalItem = new HashMap<>(item);
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName1).withItem(item);
        amazonDynamoDb.putItem(putItemRequest);
        assertThat(putItemRequest.getItem(), is(originalItem));
        assertEquals(tableName1, putItemRequest.getTableName());

        // put item in ctx2
        mtContext.setContext("ctx2");
        amazonDynamoDb
            .putItem(new PutItemRequest().withTableName(tableName1)
                .withItem(ItemBuilder.builder(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)
                        .someField(S, "someValue2")
                        .build()));

        // get item from ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item1 = getItem(amazonDynamoDb,
                tableName1,
                HASH_KEY_VALUE,
                HASH_KEY_ATTR_TYPE,
                Optional.empty());
        assertItemValue("someValue1", item1);

        // query item from ctx1
        String keyConditionExpression = "#name = :value";
        Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#name", HASH_KEY_FIELD);
        Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap
            .of(":value", createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE));
        QueryRequest queryRequest = new QueryRequest().withTableName(tableName1)
            .withKeyConditionExpression(keyConditionExpression)
            .withExpressionAttributeNames(queryExpressionAttrNames)
            .withExpressionAttributeValues(queryExpressionAttrValues);
        List<Map<String, AttributeValue>> queryItems1 = amazonDynamoDb.query(queryRequest).getItems();
        assertItemValue("someValue1", queryItems1.get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression));
        assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
        assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));

        // query item from ctx using keyConditions
        QueryRequest queryRequestKeyConditions = new QueryRequest().withTableName(tableName1)
            .withKeyConditions(ImmutableMap.of(
                HASH_KEY_FIELD,
                new Condition().withComparisonOperator(EQ)
                    .withAttributeValueList(createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE))));
        List<Map<String, AttributeValue>> queryItemsKeyConditions = amazonDynamoDb
            .query(queryRequestKeyConditions).getItems();
        assertItemValue("someValue1", queryItemsKeyConditions.get(0));

        // get item from ctx2
        mtContext.setContext("ctx2");
        assertItemValue("someValue2", getItem(amazonDynamoDb,
                tableName1,
                HASH_KEY_VALUE,
                HASH_KEY_ATTR_TYPE,
                Optional.empty()));

        // query item from ctx2 using attribute name placeholders
        List<Map<String, AttributeValue>> queryItems2 = amazonDynamoDb.query(
            new QueryRequest().withTableName(tableName1).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)))).getItems();
        assertItemValue("someValue2", queryItems2.get(0));

        // query item from ctx2 using attribute name literals
        // Note: field names with '-' will fail if you use literals instead of expressionAttributeNames()
        List<Map<String, AttributeValue>> queryItems3 = amazonDynamoDb.query(
            new QueryRequest().withTableName(tableName1).withKeyConditionExpression(HASH_KEY_FIELD + " = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)))).getItems();
        assertItemValue("someValue2", queryItems3.get(0));

        // scan for an item using hk
        String filterExpression1 = "#name = :value";
        Map<String, String> scanExpressionAttrNames1 = ImmutableMap.of("#name", HASH_KEY_FIELD);
        Map<String, AttributeValue> scanExpressionAttrValues1 = ImmutableMap
            .of(":value", createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE));
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName1)
            .withFilterExpression(filterExpression1)
            .withExpressionAttributeNames(scanExpressionAttrNames1)
            .withExpressionAttributeValues(scanExpressionAttrValues1);
        assertItemValue("someValue2", amazonDynamoDb.scan(scanRequest).getItems().get(0));
        assertEquals(tableName1, scanRequest.getTableName());
        assertThat(scanRequest.getFilterExpression(), is(filterExpression1));
        assertThat(scanRequest.getExpressionAttributeNames(), is(scanExpressionAttrNames1));
        assertThat(scanRequest.getExpressionAttributeValues(), is(scanExpressionAttrValues1));

        // scan item from ctx using scanFilter
        ScanRequest scanRequestKeyConditions = new ScanRequest().withTableName(tableName1)
            .withScanFilter(ImmutableMap.of(
                HASH_KEY_FIELD,
                new Condition().withComparisonOperator(EQ)
                    .withAttributeValueList(createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE))));
        List<Map<String, AttributeValue>> scanItemsScanFilter = amazonDynamoDb
            .scan(scanRequestKeyConditions).getItems();
        assertItemValue("someValue2", scanItemsScanFilter.get(0));

        // scan for an item using non-hk
        String filterExpression2 = "#name = :value";
        Map<String, String> scanExpressionAttrNames2 = ImmutableMap.of("#name", SOME_FIELD);
        Map<String, AttributeValue> scanExpressionAttrValues2 = ImmutableMap
            .of(":value", createStringAttribute("someValue2"));
        ScanRequest scanRequest2 = new ScanRequest().withTableName(tableName1)
            .withFilterExpression(filterExpression2)
            .withExpressionAttributeNames(scanExpressionAttrNames2)
            .withExpressionAttributeValues(scanExpressionAttrValues2);
        assertItemValue("someValue2", amazonDynamoDb.scan(scanRequest2).getItems().get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(scanRequest2.getFilterExpression(), is(filterExpression2));
        assertThat(scanRequest2.getExpressionAttributeNames(), is(scanExpressionAttrNames2));
        assertThat(scanRequest2.getExpressionAttributeValues(), is(scanExpressionAttrValues2));

        // scan all
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems1 = amazonDynamoDb
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems1.size());
        assertItemValue("someValue1", scanItems1.get(0));
        mtContext.setContext("ctx2");
        List<Map<String, AttributeValue>> scanItems2 = amazonDynamoDb
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems2.size());
        assertItemValue("someValue2", scanItems2.get(0));

        // update item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> updateItemKey = new HashMap<>(
            ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)));
        Map<String, AttributeValue> originalUpdateItemKey = new HashMap<>(updateItemKey);
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .addAttributeUpdatesEntry(SOME_FIELD,
                new AttributeValueUpdate().withValue(createStringAttribute("someValue1Updated")));
        amazonDynamoDb.updateItem(updateItemRequest);
        assertItemValue("someValue1Updated",
                getItem(amazonDynamoDb, tableName1, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));
        assertThat(updateItemRequest.getKey(), is(originalUpdateItemKey));
        assertEquals(tableName1, updateItemRequest.getTableName());

        // update item in ctx2
        mtContext.setContext("ctx2");
        amazonDynamoDb.updateItem(new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(new HashMap<>(ImmutableMap.of(HASH_KEY_FIELD,
                createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE))))
            .addAttributeUpdatesEntry(SOME_FIELD,
                new AttributeValueUpdate().withValue(createStringAttribute("someValue2Updated"))));
        assertItemValue("someValue2Updated",
                getItem(amazonDynamoDb, tableName1, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));

        // conditional update, fail
        mtContext.setContext("ctx1");
        UpdateItemRequest condUpdateItemRequestFail = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .withUpdateExpression("set #name = :newValue")
            .withConditionExpression("#name = :currentValue")
            .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
            .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
            .addExpressionAttributeValuesEntry(":newValue", createStringAttribute("someValue1UpdatedAgain"));
        try {
            amazonDynamoDb.updateItem(condUpdateItemRequestFail);
            throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
        } catch (ConditionalCheckFailedException ignore) {
            // OK to ignore(?)
        }
        assertItemValue("someValue1Updated",
                getItem(amazonDynamoDb, tableName1, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));

        // conditional update, success
        mtContext.setContext("ctx1");
        UpdateItemRequest condUpdateItemRequestSuccess = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .withUpdateExpression("set #name = :newValue")
            .withConditionExpression("#name = :currentValue")
            .addExpressionAttributeNamesEntry("#name", SOME_FIELD)
            .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("someValue1Updated"))
            .addExpressionAttributeValuesEntry(":newValue", createStringAttribute("someValue1UpdatedAgain"));
        amazonDynamoDb.updateItem(condUpdateItemRequestSuccess);
        assertItemValue("someValue1UpdatedAgain",
                getItem(amazonDynamoDb, tableName1, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));

        // put item into table2 in ctx1
        mtContext.setContext("ctx1");
        amazonDynamoDb
            .putItem(new PutItemRequest().withTableName(tableName2)
                .withItem(ItemBuilder.builder(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)
                        .someField(S, "someValueTable2")
                        .build()));

        // get item from table2 in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> itemTable2
                = getItem(amazonDynamoDb, tableName2, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty());
        assertItemValue("someValueTable2", itemTable2);

        // delete item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> deleteItemKey = new HashMap<>(
            ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE)));
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName1)
            .withKey(deleteItemKey);
        amazonDynamoDb.deleteItem(deleteItemRequest);
        assertNull(getItem(amazonDynamoDb, tableName1, "someValue1Updated", HASH_KEY_ATTR_TYPE, Optional.empty()));
        mtContext.setContext("ctx2");
        assertItemValue("someValue2Updated",
                getItem(amazonDynamoDb, tableName1, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));
        mtContext.setContext("ctx1");
        assertItemValue("someValueTable2",
                getItem(amazonDynamoDb, tableName2, HASH_KEY_VALUE, HASH_KEY_ATTR_TYPE, Optional.empty()));
        assertEquals(tableName1, deleteItemRequest.getTableName());
        assertThat(deleteItemRequest.getKey(), is(originalDeleteItemKey));

        // table with hk/rk and gsi/lsi

        // put items, same hk, different rk
        Map<String, AttributeValue> table3item1 = ImmutableMap.of(HASH_KEY_FIELD,
            createAttributeValue(HASH_KEY_ATTR_TYPE, "hashKeyValue3"),
            RANGE_KEY_FIELD, createStringAttribute("rangeKeyValue3a"),
            SOME_FIELD, createStringAttribute("someValue3a"));
        Map<String, AttributeValue> table3item2 = ImmutableMap.of(HASH_KEY_FIELD,
            createAttributeValue(HASH_KEY_ATTR_TYPE, "hashKeyValue3"),
            RANGE_KEY_FIELD, createStringAttribute("rangeKeyValue3b"),
            SOME_FIELD, createStringAttribute("someValue3b"));
        amazonDynamoDb.putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item1));
        amazonDynamoDb
            .putItem(new PutItemRequest().withTableName(tableName3).withItem(new HashMap<>(table3item2)));

        // get item
        GetItemRequest getItemRequest4 = new GetItemRequest().withTableName(tableName3).withKey(new HashMap<>(
            ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(HASH_KEY_ATTR_TYPE, "hashKeyValue3"),
                RANGE_KEY_FIELD, createStringAttribute("rangeKeyValue3a"))));
        assertThat(amazonDynamoDb.getItem(getItemRequest4).getItem(), is(table3item1));

        // delete and create table and verify no leftover data
        deleteTable(mtContext, amazonDynamoDb, "ctx1", tableName1);
        createTable(mtContext, amazonDynamoDb, "ctx1", createTableRequest1);
        List<Map<String, AttributeValue>> scanItems3 = amazonDynamoDb
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(0, scanItems3.size());

        // query hk and rk
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> table3item3 = new HashMap<>(ImmutableMap.of(
            HASH_KEY_FIELD, createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE),
            RANGE_KEY_FIELD, createStringAttribute("rangeKeyValue"),
            INDEX_FIELD, createStringAttribute("indexFieldValue")));
        amazonDynamoDb.putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item3));
        List<Map<String, AttributeValue>> queryItems6 = amazonDynamoDb.query(
            new QueryRequest().withTableName(tableName3)
                .withKeyConditionExpression("#name = :value AND #name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD,
                    "#name2", RANGE_KEY_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE),
                    ":value2", createStringAttribute("rangeKeyValue")))).getItems();
        assertEquals(1, queryItems6.size());
        Map<String, AttributeValue> queryItem6 = queryItems6.get(0);
        assertThat(queryItem6, is(table3item3));

        // query hk with filter expression on someField
        List<Map<String, AttributeValue>> queryItemsFe = amazonDynamoDb.query(
            new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value")
                .withFilterExpression("#name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", SOME_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createAttributeValue(HASH_KEY_ATTR_TYPE, "hashKeyValue3"),
                    ":value2", createStringAttribute("someValue3a")))).getItems();
        assertEquals(1, queryItemsFe.size());

        // query on gsi
        List<Map<String, AttributeValue>> queryItems4 = amazonDynamoDb.query(
            new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", INDEX_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createStringAttribute("indexFieldValue")))
                .withIndexName("testgsi")).getItems();
        assertEquals(1, queryItems4.size());
        Map<String, AttributeValue> queryItem4 = queryItems4.get(0);
        assertThat(queryItem4, is(table3item3));

        // query on lsi
        QueryRequest queryRequest5 = new QueryRequest().withTableName(tableName3)
            .withKeyConditionExpression("#name = :value and #name2 = :value2")
            .withExpressionAttributeNames(ImmutableMap.of("#name", HASH_KEY_FIELD, "#name2", INDEX_FIELD))
            .withExpressionAttributeValues(ImmutableMap.of(":value",
                createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE),
                ":value2", createStringAttribute("indexFieldValue")))
            .withIndexName("testlsi");
        List<Map<String, AttributeValue>> queryItems5 = amazonDynamoDb.query(queryRequest5).getItems();
        assertEquals(1, queryItems5.size());
        Map<String, AttributeValue> queryItem5 = queryItems5.get(0);
        assertThat(queryItem5, is(table3item3));

        // scan on hk and rk
        String filterExpressionHkRk = "#name1 = :value1 AND #name2 = :value2";
        Map<String, String> scanExpressionAttrNamesHkRk = ImmutableMap
            .of("#name1", HASH_KEY_FIELD, "#name2", RANGE_KEY_FIELD);
        Map<String, AttributeValue> scanExpressionAttrValuesHkRk = ImmutableMap
            .of(":value1", createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE),
                ":value2", createStringAttribute("rangeKeyValue"));
        ScanRequest scanRequestHkRk = new ScanRequest().withTableName(tableName3)
            .withFilterExpression(filterExpressionHkRk)
            .withExpressionAttributeNames(scanExpressionAttrNamesHkRk)
            .withExpressionAttributeValues(scanExpressionAttrValuesHkRk);
        List<Map<String, AttributeValue>> scanItemsHkRk = amazonDynamoDb
            .scan(scanRequestHkRk).getItems();
        assertEquals(1, scanItemsHkRk.size());
        Map<String, AttributeValue> scanItemHkRk = scanItemsHkRk.get(0);
        assertThat(scanItemHkRk, is(table3item3));

        // scan all on table with gsi and confirm fields with gsi is saved
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems4 = amazonDynamoDb.scan(
            new ScanRequest().withTableName(tableName3).withFilterExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", INDEX_FIELD))
                .withExpressionAttributeValues(ImmutableMap.of(":value",
                    createStringAttribute("indexFieldValue"))))
            .getItems();
        assertEquals(1, scanItems4.size());
        Map<String, AttributeValue> scanItem4 = scanItems4.get(0);
        assertThat(scanItem4, is(table3item3));

        // log end
        LOG.info("END test " + testDescription);
    }

    private void teardown(AmazonDynamoDB rootAmazonDynamoDb) {
        for (String tableName : rootAmazonDynamoDb.listTables().getTableNames()) {
            String prefix = getPrefix();
            if (tableName.startsWith(prefix)) {
                new AmazonDynamoDbAdminUtils(rootAmazonDynamoDb)
                    .deleteTableIfExists(tableName, getPollInterval(), TIMEOUT_SECONDS);
            }
        }
    }

    private void assertItemValue(String expectedValue, Map<String, AttributeValue> actualItem) {
        assertThat(actualItem, is(ImmutableMap.of(HASH_KEY_FIELD,
            createAttributeValue(HASH_KEY_ATTR_TYPE, HASH_KEY_VALUE),
            SOME_FIELD, createStringAttribute(expectedValue))));
    }

    private void deleteTable(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        String tenantId,
        String tableName) {
        mtContext.setContext(tenantId);
        new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
            .deleteTableIfExists(tableName, getPollInterval(), TIMEOUT_SECONDS);
    }

    private void createTable(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        String context,
        CreateTableRequest createTableRequest) {
        mtContext.setContext(context);
        new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
            .createTableIfNotExists(createTableRequest, getPollInterval());
    }

    private void recreateTable(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        String context,
        CreateTableRequest createTableRequest) {
        mtContext.setContext(context);
        deleteTable(mtContext, amazonDynamoDb, context, createTableRequest.getTableName());
        createTable(mtContext, amazonDynamoDb, context, createTableRequest);
    }

    private int getPollInterval() {
        return IS_LOCAL_DYNAMO ? 0 : 1;
    }

    private String buildTableName(int ordinal) {
        return getPrefix() + "Table" + ordinal;
    }

    private String getPrefix() {
        return (IS_LOCAL_DYNAMO ? "" : "oktodelete-" + TestAmazonDynamoDbAdminUtils.getLocalHost() + "-");
    }

}
