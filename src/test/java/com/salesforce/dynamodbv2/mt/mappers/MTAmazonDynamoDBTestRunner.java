/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
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
import com.salesforce.dynamodbv2.AmazonDynamoDBLocal;
import com.salesforce.dynamodbv2.TestAmazonDynamoDBAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author msgroi
 */
public class MTAmazonDynamoDBTestRunner {

    private final MTAmazonDynamoDBContextProvider mtContext;
    protected static String hashKeyField = "hashKeyField";
    protected final Supplier<AmazonDynamoDB> amazonDynamoDBSupplier;
    protected final int timeoutSeconds = 180;
    private final boolean isLocalDynamo;
    private final String tableName0;
    private final String tableName1;
    private final String tableName2;
    private final String tableName3;
    private final List<Map<String, String>> ctxTablePairs;

    public MTAmazonDynamoDBTestRunner(MTAmazonDynamoDBContextProvider mtContext,
                               Supplier<AmazonDynamoDB> amazonDynamoDBSupplier,
                               boolean isLocalDynamo) {
        this.mtContext = mtContext;
        this.amazonDynamoDBSupplier = amazonDynamoDBSupplier;
        this.isLocalDynamo = isLocalDynamo;
        tableName0 = buildTableName(0);
        tableName1 = buildTableName(1);
        tableName2 = buildTableName(2);
        tableName3 = buildTableName(3);
        ctxTablePairs = ImmutableList.of(
            ImmutableMap.of("ctx1", tableName0),
            ImmutableMap.of("ctx1", tableName1),
            ImmutableMap.of("ctx2", tableName1),
            ImmutableMap.of("ctx1", tableName2),
            ImmutableMap.of("ctx1", tableName3)
        );
    }

    static AmazonDynamoDB getLocalAmazonDynamoDB() {
        return AmazonDynamoDBLocal.getAmazonDynamoDBLocal();
    }

    void runAll() {
        setup();
        run();
        teardown();
    }

    private AmazonDynamoDB getAmazonDynamoDBSupplier() {
        return this.amazonDynamoDBSupplier.get();
    }

    void setup() {
        ctxTablePairs.forEach(ctxTablePair -> {
            Map.Entry<String, String> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
            recreateTable(ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
        });
    }

    /*
     * Create a table in different contexts, insert a record into each with the same key and different value.  Query for
     * the record in each context and confirm you get the right result.
     */

    void run() {
        // create table with hk only
        mtContext.setContext("ctx1");
        deleteTable("ctx1", tableName0);
        CreateTableRequest createTableRequest1 = new CreateTableRequest()
                .withTableName(tableName0)
                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        getAmazonDynamoDBSupplier().createTable(createTableRequest1);
        assertEquals(tableName0, createTableRequest1.getTableName());
        new TestAmazonDynamoDBAdminUtils(getAmazonDynamoDBSupplier()).awaitTableActive(tableName0, getPollInterval(), timeoutSeconds);
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withTableName(tableName0);
        getAmazonDynamoDBSupplier().deleteTable(deleteTableRequest);
        assertEquals(tableName0, createTableRequest1.getTableName());

        // describe table in ctx1
        mtContext.setContext("ctx1");
        assertEquals(tableName1, getAmazonDynamoDBSupplier().describeTable(tableName1).getTable().getTableName());

        // describe table in ctx2
        mtContext.setContext("ctx2");
        assertEquals(tableName1, getAmazonDynamoDBSupplier().describeTable(tableName1).getTable().getTableName());

        // put item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item = createItem("someValue1");
        Map<String, AttributeValue> originalItem = new HashMap<>(item);
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName1).withItem(item);
        getAmazonDynamoDBSupplier().putItem(putItemRequest);
        assertThat(putItemRequest.getItem(), is(originalItem));
        assertEquals(tableName1, putItemRequest.getTableName());

        // put item in ctx2
        mtContext.setContext("ctx2");
        getAmazonDynamoDBSupplier().putItem(new PutItemRequest().withTableName(tableName1).withItem(createItem("someValue2")));

        // get item from ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item1 = getItem(tableName1);
        assertItemValue("someValue1", item1);

        // query item from ctx1
        String keyConditionExpression = "#name = :value";
        Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#name", hashKeyField);
        Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap.of(":value", new AttributeValue().withS("hashKeyValue"));
        QueryRequest queryRequest = new QueryRequest().withTableName(tableName1).withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeNames(queryExpressionAttrNames)
                .withExpressionAttributeValues(queryExpressionAttrValues);
        List<Map<String, AttributeValue>> queryItems1 = getAmazonDynamoDBSupplier().query(queryRequest).getItems();
        assertItemValue("someValue1", queryItems1.get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression));
        assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
        assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));

        // get item from ctx2
        mtContext.setContext("ctx2");
        assertItemValue("someValue2", getItem(tableName1));

        // query item from ctx2 using attribute name placeholders
        List<Map<String, AttributeValue>> queryItems2 = getAmazonDynamoDBSupplier().query(
                new QueryRequest().withTableName(tableName1).withKeyConditionExpression("#name = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hashKeyValue")))).getItems();
        assertItemValue("someValue2", queryItems2.get(0));

        // query item from ctx2 using attribute name literals
        List<Map<String, AttributeValue>> queryItems3 = getAmazonDynamoDBSupplier().query(
                new QueryRequest().withTableName(tableName1).withKeyConditionExpression(hashKeyField + " = :value")
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hashKeyValue")))).getItems();
        assertItemValue("someValue2", queryItems3.get(0));

        // scan for an item using hk
        String filterExpression1 = "#name = :value";
        Map<String, String> scanExpressionAttrNames1 = ImmutableMap.of("#name", hashKeyField);
        Map<String, AttributeValue> scanExpressionAttrValues1 = ImmutableMap.of(":value", new AttributeValue().withS("hashKeyValue"));
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName1).withFilterExpression(filterExpression1)
                .withExpressionAttributeNames(scanExpressionAttrNames1)
                .withExpressionAttributeValues(scanExpressionAttrValues1);
        assertItemValue("someValue2", getAmazonDynamoDBSupplier().scan(scanRequest).getItems().get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(scanRequest.getFilterExpression(), is(filterExpression1));
        assertThat(scanRequest.getExpressionAttributeNames(), is(scanExpressionAttrNames1));
        assertThat(scanRequest.getExpressionAttributeValues(), is(scanExpressionAttrValues1));

        // scan for an item using non-hk
        String filterExpression2 = "#name = :value";
        Map<String, String> scanExpressionAttrNames2 = ImmutableMap.of("#name", "someField");
        Map<String, AttributeValue> scanExpressionAttrValues2 = ImmutableMap.of(":value", new AttributeValue().withS("someValue2"));
        ScanRequest scanRequest2 = new ScanRequest().withTableName(tableName1).withFilterExpression(filterExpression2)
                .withExpressionAttributeNames(scanExpressionAttrNames2)
                .withExpressionAttributeValues(scanExpressionAttrValues2);
        assertItemValue("someValue2", getAmazonDynamoDBSupplier().scan(scanRequest2).getItems().get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(scanRequest2.getFilterExpression(), is(filterExpression2));
        assertThat(scanRequest2.getExpressionAttributeNames(), is(scanExpressionAttrNames2));
        assertThat(scanRequest2.getExpressionAttributeValues(), is(scanExpressionAttrValues2));

        // scan all
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems1 = getAmazonDynamoDBSupplier().scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems1.size());
        assertItemValue("someValue1", scanItems1.get(0));
        mtContext.setContext("ctx2");
        List<Map<String, AttributeValue>> scanItems2 = getAmazonDynamoDBSupplier().scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems2.size());
        assertItemValue("someValue2", scanItems2.get(0));

        // update item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> updateItemKey = new HashMap<>(ImmutableMap.of(hashKeyField, new AttributeValue("hashKeyValue")));
        Map<String, AttributeValue> originalUpdateItemKey = new HashMap<>(updateItemKey);
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName1)
                .withKey(updateItemKey)
                .addAttributeUpdatesEntry("someField", new AttributeValueUpdate().withValue(new AttributeValue().withS("someValue1Updated")));
        getAmazonDynamoDBSupplier().updateItem(updateItemRequest);
        assertItemValue("someValue1Updated", getItem(tableName1));
        assertThat(updateItemRequest.getKey(), is(originalUpdateItemKey));
        assertEquals(tableName1, updateItemRequest.getTableName());

        // update item in ctx2
        mtContext.setContext("ctx2");
        getAmazonDynamoDBSupplier().updateItem(new UpdateItemRequest()
                .withTableName(tableName1)
                .withKey(new HashMap<>(ImmutableMap.of(hashKeyField, new AttributeValue("hashKeyValue"))))
                .addAttributeUpdatesEntry("someField", new AttributeValueUpdate().withValue(new AttributeValue().withS("someValue2Updated"))));
        assertItemValue("someValue2Updated", getItem(tableName1));

        // put item into table2 in ctx1
        mtContext.setContext("ctx1");
        getAmazonDynamoDBSupplier().putItem(new PutItemRequest().withTableName(tableName2).withItem(createItem("someValueTable2")));

        // get item from table2 in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> itemTable2 = getItem(tableName2);
        assertItemValue("someValueTable2", itemTable2);

        // delete item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> deleteItemKey = new HashMap<>(ImmutableMap.of(hashKeyField, new AttributeValue("someValue1Updated")));
        Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName1).withKey(deleteItemKey);
        getAmazonDynamoDBSupplier().deleteItem(deleteItemRequest);
        assertNull(getItem(tableName1, "someValue1Updated"));
        mtContext.setContext("ctx2");
        assertItemValue("someValue2Updated", getItem(tableName1));
        mtContext.setContext("ctx1");
        assertItemValue("someValueTable2", getItem(tableName2));
        assertEquals(tableName1, deleteItemRequest.getTableName());
        assertThat(deleteItemRequest.getKey(), is(originalDeleteItemKey));

        // create table with hk/rk and gsi/lsi
        mtContext.setContext("ctx1");
        deleteTable("ctx1", tableName3);
        String rangeKeyField = "rangeKeyField";
        String indexField = "indexField";
        CreateTableRequest createTableRequest2 = new CreateTableRequest()
                .withTableName(tableName3)
                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, ScalarAttributeType.S),
                                          new AttributeDefinition(rangeKeyField, ScalarAttributeType.S),
                                          new AttributeDefinition(indexField, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                               new KeySchemaElement(rangeKeyField, KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(indexField, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                       new KeySchemaElement(indexField, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                ;
        getAmazonDynamoDBSupplier().createTable(createTableRequest2);
        assertEquals(tableName3, createTableRequest2.getTableName());
        new TestAmazonDynamoDBAdminUtils(getAmazonDynamoDBSupplier()).awaitTableActive(tableName3, getPollInterval(), timeoutSeconds);

        // put items, same hk, different rk
        Map<String, AttributeValue> table3item1 = ImmutableMap.of(hashKeyField, new AttributeValue("hashKeyValue3"),
                                                                  rangeKeyField, new AttributeValue("rangeKeyValue3a"),
                                                                  "someField", new AttributeValue("someValue3a"));
        Map<String, AttributeValue> table3item2 = ImmutableMap.of(hashKeyField, new AttributeValue("hashKeyValue3"),
                                                                  rangeKeyField, new AttributeValue("rangeKeyValue3b"),
                                                                  "someField", new AttributeValue("someValue3b"));
        getAmazonDynamoDBSupplier().putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item1));
        getAmazonDynamoDBSupplier().putItem(new PutItemRequest().withTableName(tableName3).withItem(new HashMap<>(table3item2)));

        // get item
        GetItemRequest getItemRequest4 = new GetItemRequest().withTableName(tableName3).withKey(new HashMap<>(
                ImmutableMap.of(hashKeyField, new AttributeValue("hashKeyValue3"),
                                rangeKeyField, new AttributeValue("rangeKeyValue3a"))));
        assertThat(getAmazonDynamoDBSupplier().getItem(getItemRequest4).getItem(), is(table3item1));

        // delete and create table and verify no leftover data
        deleteTable("ctx1", tableName1);
        createTable("ctx1", tableName1);
        List<Map<String, AttributeValue>> scanItems3 = getAmazonDynamoDBSupplier().scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(0, scanItems3.size());

        // query on gsi
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> table3item3 = new HashMap<>(ImmutableMap.of(
                hashKeyField, new AttributeValue("hashKeyValue"),
                rangeKeyField, new AttributeValue("rangeKeyValue"),
                indexField, new AttributeValue("indexFieldValue")));
        getAmazonDynamoDBSupplier().putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item3));
        List<Map<String, AttributeValue>> queryItems4 = getAmazonDynamoDBSupplier().query(
                new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", indexField))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("indexFieldValue")))
                        .withIndexName("testgsi")).getItems();
        assertEquals(1, queryItems4.size());
        Map<String, AttributeValue> queryItem4 = queryItems4.get(0);
        assertEquals("hashKeyValue", queryItem4.get(hashKeyField).getS());
        assertThat(queryItem4, is(table3item3));

        // query on lsi
        List<Map<String, AttributeValue>> queryItems5 = getAmazonDynamoDBSupplier().query(
                new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value and #name2 = :value2")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField, "#name2", indexField))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("hashKeyValue"),
                                                                   ":value2", new AttributeValue().withS("indexFieldValue")))
                        .withIndexName("testlsi")).getItems();
        assertEquals(1, queryItems5.size());
        Map<String, AttributeValue> queryItem5 = queryItems5.get(0);
        assertEquals("hashKeyValue", queryItem5.get(hashKeyField).getS());
        assertThat(queryItem5, is(table3item3));

        // scan all on table with gsi and confirm fields with gsi is saved
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems4 = getAmazonDynamoDBSupplier().scan(
                new ScanRequest().withTableName(tableName3).withFilterExpression("#name = :value")
                        .withExpressionAttributeNames(ImmutableMap.of("#name", indexField))
                        .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue().withS("indexFieldValue")))).getItems();
        assertEquals(1, scanItems4.size());
        Map<String, AttributeValue> scanItem4 = scanItems4.get(0);
        assertThat(scanItem4, is(table3item3));


    }
    void teardown() {
        deleteTables(ctxTablePairs);
    }

    protected void deleteTables(List<Map<String, String>> ctxPairs) {
        ctxPairs.forEach(ctxTablePair -> {
            Map.Entry<String, String> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
            deleteTable(ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
        });
    }

    private Map<String, AttributeValue> createItem(String value) {
        return createItem(hashKeyField, "hashKeyValue" , "someField", value);
    }

    protected Map<String, AttributeValue> createItem(String hashKeyField, String hashKeyValue, String someField, String someFieldValue) {
        return new HashMap<>(ImmutableMap.of(
                hashKeyField, new AttributeValue(hashKeyValue),
                someField, new AttributeValue(someFieldValue)));
    }

    private Map<String, AttributeValue> getItem(String tableName) {
        return getItem(tableName, "hashKeyValue");
    }

    private Map<String, AttributeValue> getItem(String tableName, String value) {
        Map<String, AttributeValue> keys = new HashMap<>(ImmutableMap.of(hashKeyField, new AttributeValue(value)));
        Map<String, AttributeValue> originalKeys = new HashMap<>(ImmutableMap.of(hashKeyField, new AttributeValue(value)));
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName).withKey(keys);
        GetItemResult getItemResult = getAmazonDynamoDBSupplier().getItem(getItemRequest);
        assertEquals(tableName, getItemRequest.getTableName());
        assertThat(getItemRequest.getKey(), is(originalKeys));
        return getItemResult.getItem();
    }

    private void assertItemValue(String expectedValue, Map<String, AttributeValue> actualItem) {
        assertEquals("hashKeyValue", actualItem.get(hashKeyField).getS());
        Map<String, AttributeValue> expectedItem = ImmutableMap.of(
                hashKeyField, new AttributeValue().withS("hashKeyValue"),
                "someField", new AttributeValue().withS(expectedValue));
        assertThat(actualItem, is(expectedItem));
    }

    private void deleteTable(String tenantId, String tableName) {
        mtContext.setContext(tenantId);
        new TestAmazonDynamoDBAdminUtils(getAmazonDynamoDBSupplier()).deleteTableIfNotExists(tableName, getPollInterval(), timeoutSeconds);
    }

    protected void createTable(String context, String tableName) {
        mtContext.setContext(context);
        new TestAmazonDynamoDBAdminUtils(getAmazonDynamoDBSupplier()).createTableIfNotExists(new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withTableName(tableName), getPollInterval());
    }

    protected void recreateTable(String context, String tableName) {
        mtContext.setContext(context);
        deleteTable(context, tableName);
        createTable(context, tableName);
    }

    protected int getPollInterval() {
        return isLocalDynamo ? 0 : 1;
    }

    private String buildTableName(int ordinal) {
        return buildTableName(getClass().getSimpleName() + ordinal);
    }

    private String buildTableName(String table) {
        return "oktodelete-" + (isLocalDynamo ? "" : TestAmazonDynamoDBAdminUtils.getLocalHost() + "-") + table;
    }

}