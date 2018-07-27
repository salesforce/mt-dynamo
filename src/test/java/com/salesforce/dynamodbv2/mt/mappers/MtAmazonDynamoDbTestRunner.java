/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.OperationType.INSERT;
import static com.amazonaws.services.dynamodbv2.model.OperationType.MODIFY;
import static com.amazonaws.services.dynamodbv2.model.OperationType.REMOVE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
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
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.TestAmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbTestRunner {

    private static final Logger log = LoggerFactory.getLogger(MtAmazonDynamoDbTestRunner.class);
    private final MtAmazonDynamoDbContextProvider mtContext;
    protected static String hashKeyField = "hashKeyField";
    static String rangeKeyField = "rangeKeyField";
    static String indexField = "indexField";
    private static final String someField = "someField";
    protected final Supplier<AmazonDynamoDB> amazonDynamoDbSupplier;
    private final AmazonDynamoDB amazonDynamoDb;
    private final AmazonDynamoDB rootAmazonDynamoDb;
    protected final int timeoutSeconds = 600;
    private final boolean isLocalDynamo;
    private final CreateTableRequest createTableRequest1;
    private final CreateTableRequest createTableRequest2;
    private final CreateTableRequest createTableRequest3;
    private final CreateTableRequest createTableRequest4;
    private final String tableName1;
    private final String tableName2;
    private final String tableName3;
    private final String tableName4;
    private final List<Map<String, CreateTableRequest>> ctxTablePairs;
    private final ScalarAttributeType hashKeyAttrType;
    private final MtAmazonDynamoDbStreamTestRunner streamTestRunner;

    protected MtAmazonDynamoDbTestRunner(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        AmazonDynamoDB rootAmazonDynamoDb,
        AWSCredentialsProvider awsCredentialsProvider,
        boolean isLocalDynamo) {
        this(mtContext,
            amazonDynamoDb,
            rootAmazonDynamoDb,
            null,
            awsCredentialsProvider,
            isLocalDynamo,
            S);
    }

    MtAmazonDynamoDbTestRunner(MtAmazonDynamoDbContextProvider mtContext,
        AmazonDynamoDB amazonDynamoDb,
        AmazonDynamoDB rootAmazonDynamoDb,
        AmazonDynamoDBStreams rootAmazonDynamoDbStreams,
        AWSCredentialsProvider awsCredentialsProvider,
        boolean isLocalDynamo,
        ScalarAttributeType hashKeyAttrType) {
        this.mtContext = mtContext;
        this.amazonDynamoDb = amazonDynamoDb;
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
        this.amazonDynamoDbSupplier = () -> this.amazonDynamoDb;
        this.isLocalDynamo = isLocalDynamo;
        this.hashKeyAttrType = hashKeyAttrType;
        tableName1 = buildTableName(1);
        tableName2 = buildTableName(2);
        tableName3 = buildTableName(3);
        tableName4 = buildTableName(5);
        createTableRequest1 = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, hashKeyAttrType))
            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withTableName(tableName1);
        createTableRequest2 = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, hashKeyAttrType))
            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withTableName(tableName2);
        createTableRequest3 = new CreateTableRequest()
            .withTableName(tableName3)
            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, hashKeyAttrType),
                new AttributeDefinition(rangeKeyField, S),
                new AttributeDefinition(indexField, S))
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
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
        createTableRequest4 = new CreateTableRequest()
            .withTableName(tableName4)
            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, B))
            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        ctxTablePairs = new ArrayList<>(ImmutableList.of(
            ImmutableMap.of("ctx1", createTableRequest1),
            ImmutableMap.of("ctx2", createTableRequest1),
            ImmutableMap.of("ctx1", createTableRequest2),
            ImmutableMap.of("ctx1", createTableRequest3),
            ImmutableMap.of("ctx3", createTableRequest1),
            ImmutableMap.of("ctx4", createTableRequest1)
        ));
        this.streamTestRunner = new MtAmazonDynamoDbStreamTestRunner(amazonDynamoDbSupplier.get(),
            rootAmazonDynamoDb,
            rootAmazonDynamoDbStreams,
            awsCredentialsProvider,
            getExpectedMtRecords()
        );
    }

    static AmazonDynamoDB getLocalAmazonDynamoDb() {
        return AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    }

    static AmazonDynamoDBStreams getLocalAmazonDynamoDbStreams() {
        return AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal();
    }

    void runAll() {
        setup();
        run();
        runWithoutLogging();
        teardown();
    }

    private AmazonDynamoDB getAmazonDynamoDbSupplier() {
        return amazonDynamoDb;
    }

    void setup() {
        setup(ctxTablePairs);
        streamTestRunner.startStreamWorker();
    }

    private void setup(List<Map<String, CreateTableRequest>> ctxTablePairs) {
        for (Map<String, CreateTableRequest> ctxTablePair : ctxTablePairs) {
            Entry<String, CreateTableRequest> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
            recreateTable(ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
        }
    }

    void run() {
        // log start
        String testDescription = getAmazonDynamoDbSupplier() + " with hashkey type=" + hashKeyAttrType;
        log.info("START test " + testDescription);

        // table with hk only

        // describe table in ctx1
        mtContext.setContext("ctx1");
        assertEquals(tableName1, getAmazonDynamoDbSupplier().describeTable(tableName1).getTable().getTableName());

        // describe table in ctx2
        mtContext.setContext("ctx2");
        assertEquals(tableName1, getAmazonDynamoDbSupplier().describeTable(tableName1).getTable().getTableName());

        // put item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item = createItem("someValue1");
        Map<String, AttributeValue> originalItem = new HashMap<>(item);
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName1).withItem(item);
        getAmazonDynamoDbSupplier().putItem(putItemRequest);
        assertThat(putItemRequest.getItem(), is(originalItem));
        assertEquals(tableName1, putItemRequest.getTableName());

        // put item in ctx2
        mtContext.setContext("ctx2");
        getAmazonDynamoDbSupplier()
            .putItem(new PutItemRequest().withTableName(tableName1).withItem(createItem("someValue2")));

        // get item from ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item1 = getItem(tableName1);
        assertItemValue("someValue1", item1);

        // query item from ctx1
        String keyConditionExpression = "#name = :value";
        Map<String, String> queryExpressionAttrNames = ImmutableMap.of("#name", hashKeyField);
        Map<String, AttributeValue> queryExpressionAttrValues = ImmutableMap
            .of(":value", createAttribute("hashKeyValue"));
        QueryRequest queryRequest = new QueryRequest().withTableName(tableName1)
            .withKeyConditionExpression(keyConditionExpression)
            .withExpressionAttributeNames(queryExpressionAttrNames)
            .withExpressionAttributeValues(queryExpressionAttrValues);
        List<Map<String, AttributeValue>> queryItems1 = getAmazonDynamoDbSupplier().query(queryRequest).getItems();
        assertItemValue("someValue1", queryItems1.get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(queryRequest.getKeyConditionExpression(), is(keyConditionExpression));
        assertThat(queryRequest.getExpressionAttributeNames(), is(queryExpressionAttrNames));
        assertThat(queryRequest.getExpressionAttributeValues(), is(queryExpressionAttrValues));

        // query item from ctx using keyConditions
        QueryRequest queryRequestKeyConditions = new QueryRequest().withTableName(tableName1)
            .withKeyConditions(ImmutableMap.of(
                hashKeyField,
                new Condition().withComparisonOperator(EQ).withAttributeValueList(createAttribute("hashKeyValue"))));
        List<Map<String, AttributeValue>> queryItemsKeyConditions = getAmazonDynamoDbSupplier()
            .query(queryRequestKeyConditions).getItems();
        assertItemValue("someValue1", queryItemsKeyConditions.get(0));

        // get item from ctx2
        mtContext.setContext("ctx2");
        assertItemValue("someValue2", getItem(tableName1));

        // query item from ctx2 using attribute name placeholders
        List<Map<String, AttributeValue>> queryItems2 = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName1).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createAttribute("hashKeyValue")))).getItems();
        assertItemValue("someValue2", queryItems2.get(0));

        // query item from ctx2 using attribute name literals
        // Note: field names with '-' will fail if you use literals instead of expressionAttributeNames()
        List<Map<String, AttributeValue>> queryItems3 = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName1).withKeyConditionExpression(hashKeyField + " = :value")
                .withExpressionAttributeValues(ImmutableMap.of(":value", createAttribute("hashKeyValue")))).getItems();
        assertItemValue("someValue2", queryItems3.get(0));

        // scan for an item using hk
        String filterExpression1 = "#name = :value";
        Map<String, String> scanExpressionAttrNames1 = ImmutableMap.of("#name", hashKeyField);
        Map<String, AttributeValue> scanExpressionAttrValues1 = ImmutableMap
            .of(":value", createAttribute("hashKeyValue"));
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName1).withFilterExpression(filterExpression1)
            .withExpressionAttributeNames(scanExpressionAttrNames1)
            .withExpressionAttributeValues(scanExpressionAttrValues1);
        assertItemValue("someValue2", getAmazonDynamoDbSupplier().scan(scanRequest).getItems().get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(scanRequest.getFilterExpression(), is(filterExpression1));
        assertThat(scanRequest.getExpressionAttributeNames(), is(scanExpressionAttrNames1));
        assertThat(scanRequest.getExpressionAttributeValues(), is(scanExpressionAttrValues1));

        // scan item from ctx using scanFilter
        ScanRequest scanRequestKeyConditions = new ScanRequest().withTableName(tableName1)
            .withScanFilter(ImmutableMap.of(
                hashKeyField,
                new Condition().withComparisonOperator(EQ).withAttributeValueList(createAttribute("hashKeyValue"))));
        List<Map<String, AttributeValue>> scanItemsScanFilter = getAmazonDynamoDbSupplier()
            .scan(scanRequestKeyConditions).getItems();
        assertItemValue("someValue2", scanItemsScanFilter.get(0));

        // scan for an item using non-hk
        String filterExpression2 = "#name = :value";
        Map<String, String> scanExpressionAttrNames2 = ImmutableMap.of("#name", someField);
        Map<String, AttributeValue> scanExpressionAttrValues2 = ImmutableMap
            .of(":value", createStringAttribute("someValue2"));
        ScanRequest scanRequest2 = new ScanRequest().withTableName(tableName1).withFilterExpression(filterExpression2)
            .withExpressionAttributeNames(scanExpressionAttrNames2)
            .withExpressionAttributeValues(scanExpressionAttrValues2);
        assertItemValue("someValue2", getAmazonDynamoDbSupplier().scan(scanRequest2).getItems().get(0));
        assertEquals(tableName1, queryRequest.getTableName());
        assertThat(scanRequest2.getFilterExpression(), is(filterExpression2));
        assertThat(scanRequest2.getExpressionAttributeNames(), is(scanExpressionAttrNames2));
        assertThat(scanRequest2.getExpressionAttributeValues(), is(scanExpressionAttrValues2));

        // scan all
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems1 = getAmazonDynamoDbSupplier()
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems1.size());
        assertItemValue("someValue1", scanItems1.get(0));
        mtContext.setContext("ctx2");
        List<Map<String, AttributeValue>> scanItems2 = getAmazonDynamoDbSupplier()
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(1, scanItems2.size());
        assertItemValue("someValue2", scanItems2.get(0));

        // update item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> updateItemKey = new HashMap<>(
            ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue")));
        Map<String, AttributeValue> originalUpdateItemKey = new HashMap<>(updateItemKey);
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .addAttributeUpdatesEntry(someField,
                new AttributeValueUpdate().withValue(createStringAttribute("someValue1Updated")));
        getAmazonDynamoDbSupplier().updateItem(updateItemRequest);
        assertItemValue("someValue1Updated", getItem(tableName1));
        assertThat(updateItemRequest.getKey(), is(originalUpdateItemKey));
        assertEquals(tableName1, updateItemRequest.getTableName());

        // update item in ctx2
        mtContext.setContext("ctx2");
        getAmazonDynamoDbSupplier().updateItem(new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(new HashMap<>(ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue"))))
            .addAttributeUpdatesEntry(someField,
                new AttributeValueUpdate().withValue(createStringAttribute("someValue2Updated"))));
        assertItemValue("someValue2Updated", getItem(tableName1));

        // conditional update, fail
        mtContext.setContext("ctx1");
        UpdateItemRequest condUpdateItemRequestFail = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .withUpdateExpression("set #name = :newValue")
            .withConditionExpression("#name = :currentValue")
            .addExpressionAttributeNamesEntry("#name", someField)
            .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("invalidValue"))
            .addExpressionAttributeValuesEntry(":newValue", createStringAttribute("someValue1UpdatedAgain"));
        try {
            getAmazonDynamoDbSupplier().updateItem(condUpdateItemRequestFail);
            throw new RuntimeException("expected ConditionalCheckFailedException was not encountered");
        } catch (ConditionalCheckFailedException ignore) {
            // OK to ignore(?)
        }
        assertItemValue("someValue1Updated", getItem(tableName1));

        // conditional update, success
        mtContext.setContext("ctx1");
        UpdateItemRequest condUpdateItemRequestSuccess = new UpdateItemRequest()
            .withTableName(tableName1)
            .withKey(updateItemKey)
            .withUpdateExpression("set #name = :newValue")
            .withConditionExpression("#name = :currentValue")
            .addExpressionAttributeNamesEntry("#name", someField)
            .addExpressionAttributeValuesEntry(":currentValue", createStringAttribute("someValue1Updated"))
            .addExpressionAttributeValuesEntry(":newValue", createStringAttribute("someValue1UpdatedAgain"));
        getAmazonDynamoDbSupplier().updateItem(condUpdateItemRequestSuccess);
        assertItemValue("someValue1UpdatedAgain", getItem(tableName1));

        // put item into table2 in ctx1
        mtContext.setContext("ctx1");
        getAmazonDynamoDbSupplier()
            .putItem(new PutItemRequest().withTableName(tableName2).withItem(createItem("someValueTable2")));

        // get item from table2 in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> itemTable2 = getItem(tableName2);
        assertItemValue("someValueTable2", itemTable2);

        // delete item in ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> deleteItemKey = new HashMap<>(
            ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue")));
        final Map<String, AttributeValue> originalDeleteItemKey = new HashMap<>(deleteItemKey);
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName1).withKey(deleteItemKey);
        getAmazonDynamoDbSupplier().deleteItem(deleteItemRequest);
        assertNull(getItem(tableName1, "someValue1Updated"));
        mtContext.setContext("ctx2");
        assertItemValue("someValue2Updated", getItem(tableName1));
        mtContext.setContext("ctx1");
        assertItemValue("someValueTable2", getItem(tableName2));
        assertEquals(tableName1, deleteItemRequest.getTableName());
        assertThat(deleteItemRequest.getKey(), is(originalDeleteItemKey));

        // table with hk/rk and gsi/lsi

        // put items, same hk, different rk
        Map<String, AttributeValue> table3item1 = ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue3"),
            rangeKeyField, createStringAttribute("rangeKeyValue3a"),
            someField, createStringAttribute("someValue3a"));
        Map<String, AttributeValue> table3item2 = ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue3"),
            rangeKeyField, createStringAttribute("rangeKeyValue3b"),
            someField, createStringAttribute("someValue3b"));
        getAmazonDynamoDbSupplier().putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item1));
        getAmazonDynamoDbSupplier()
            .putItem(new PutItemRequest().withTableName(tableName3).withItem(new HashMap<>(table3item2)));

        // get item
        GetItemRequest getItemRequest4 = new GetItemRequest().withTableName(tableName3).withKey(new HashMap<>(
            ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue3"),
                rangeKeyField, createStringAttribute("rangeKeyValue3a"))));
        assertThat(getAmazonDynamoDbSupplier().getItem(getItemRequest4).getItem(), is(table3item1));

        // delete and create table and verify no leftover data
        deleteTable("ctx1", tableName1);
        createTable("ctx1", createTableRequest1);
        List<Map<String, AttributeValue>> scanItems3 = getAmazonDynamoDbSupplier()
            .scan(new ScanRequest().withTableName(tableName1)).getItems();
        assertEquals(0, scanItems3.size());

        // query hk and rk
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> table3item3 = new HashMap<>(ImmutableMap.of(
            hashKeyField, createAttribute("hashKeyValue"),
            rangeKeyField, createStringAttribute("rangeKeyValue"),
            indexField, createStringAttribute("indexFieldValue")));
        getAmazonDynamoDbSupplier().putItem(new PutItemRequest().withTableName(tableName3).withItem(table3item3));
        List<Map<String, AttributeValue>> queryItems6 = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName3)
                .withKeyConditionExpression("#name = :value AND #name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField, "#name2", rangeKeyField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createAttribute("hashKeyValue"),
                    ":value2", createStringAttribute("rangeKeyValue")))).getItems();
        assertEquals(1, queryItems6.size());
        Map<String, AttributeValue> queryItem6 = queryItems6.get(0);
        assertThat(queryItem6, is(table3item3));

        // query hk with filter expression on someField
        List<Map<String, AttributeValue>> queryItemsFe = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value")
                .withFilterExpression("#name2 = :value2")
                .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField, "#name2", someField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createAttribute("hashKeyValue3"),
                    ":value2", createStringAttribute("someValue3a")))).getItems();
        assertEquals(1, queryItemsFe.size());

        // query on gsi
        List<Map<String, AttributeValue>> queryItems4 = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName3).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", indexField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createStringAttribute("indexFieldValue")))
                .withIndexName("testgsi")).getItems();
        assertEquals(1, queryItems4.size());
        Map<String, AttributeValue> queryItem4 = queryItems4.get(0);
        assertThat(queryItem4, is(table3item3));

        // query on lsi
        QueryRequest queryRequest5 = new QueryRequest().withTableName(tableName3)
            .withKeyConditionExpression("#name = :value and #name2 = :value2")
            .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField, "#name2", indexField))
            .withExpressionAttributeValues(ImmutableMap.of(":value", createAttribute("hashKeyValue"),
                ":value2", createStringAttribute("indexFieldValue")))
            .withIndexName("testlsi");
        List<Map<String, AttributeValue>> queryItems5 = getAmazonDynamoDbSupplier().query(queryRequest5).getItems();
        assertEquals(1, queryItems5.size());
        Map<String, AttributeValue> queryItem5 = queryItems5.get(0);
        assertThat(queryItem5, is(table3item3));

        // scan on hk and rk
        String filterExpressionHkRk = "#name1 = :value1 AND #name2 = :value2";
        Map<String, String> scanExpressionAttrNamesHkRk = ImmutableMap
            .of("#name1", hashKeyField, "#name2", rangeKeyField);
        Map<String, AttributeValue> scanExpressionAttrValuesHkRk = ImmutableMap
            .of(":value1", createAttribute("hashKeyValue"),
                ":value2", createStringAttribute("rangeKeyValue"));
        ScanRequest scanRequestHkRk = new ScanRequest().withTableName(tableName3)
            .withFilterExpression(filterExpressionHkRk)
            .withExpressionAttributeNames(scanExpressionAttrNamesHkRk)
            .withExpressionAttributeValues(scanExpressionAttrValuesHkRk);
        List<Map<String, AttributeValue>> scanItemsHkRk = getAmazonDynamoDbSupplier().scan(scanRequestHkRk).getItems();
        assertEquals(1, scanItemsHkRk.size());
        Map<String, AttributeValue> scanItemHkRk = scanItemsHkRk.get(0);
        assertThat(scanItemHkRk, is(table3item3));

        // scan all on table with gsi and confirm fields with gsi is saved
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> scanItems4 = getAmazonDynamoDbSupplier().scan(
            new ScanRequest().withTableName(tableName3).withFilterExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", indexField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", createStringAttribute("indexFieldValue"))))
            .getItems();
        assertEquals(1, scanItems4.size());
        Map<String, AttributeValue> scanItem4 = scanItems4.get(0);
        assertThat(scanItem4, is(table3item3));

        streamTestRunner.await(30);

        // log end
        log.info("END test " + testDescription);
    }

    void runWithoutLogging() {

        // scan with paging (including empty pages)
        mtContext.setContext("ctx3");
        // insert some data for another tenant as noise
        for (int i = 0; i < 100; i++) {
            getAmazonDynamoDbSupplier().putItem(
                new PutItemRequest(tableName1, ImmutableMap.of(hashKeyField, createAttribute(String.valueOf(i)))));
        }
        mtContext.setContext("ctx4");
        Set<Integer> remaining = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            getAmazonDynamoDbSupplier().putItem(
                new PutItemRequest(tableName1, ImmutableMap.of(hashKeyField, createAttribute(String.valueOf(i)))));
            remaining.add(i);
        }
        Map<String, AttributeValue> exclusiveStartKey = null;
        do {
            ScanResult scanResult = getAmazonDynamoDbSupplier()
                .scan(new ScanRequest(tableName1).withLimit(10).withExclusiveStartKey(exclusiveStartKey));
            exclusiveStartKey = scanResult.getLastEvaluatedKey();
            List<Map<String, AttributeValue>> items = scanResult.getItems();

            if (items.isEmpty()) {
                assertTrue(remaining.isEmpty());
                assertNull(exclusiveStartKey);
            } else {
                assertTrue(items.stream() //
                    .map(i -> i.get(hashKeyField)) //
                    .map(this::getValue) //
                    .map(Integer::parseInt) //
                    .allMatch(remaining::remove));
            }
        } while (exclusiveStartKey != null);
        assertTrue(remaining.isEmpty());
    }

    private List<MtRecord> getExpectedMtRecords() {
        return ImmutableList.of(
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1")))),
            new MtRecord()
                .withContext("ctx2")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue2")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(MODIFY.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withOldImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1Updated")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(MODIFY.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withOldImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1Updated")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1UpdatedAgain")))),
            new MtRecord()
                .withContext("ctx2")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(MODIFY.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withOldImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue2")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue2Updated")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner2")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValueTable2")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner1")
                .withEventName(REMOVE.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue")))
                    .withOldImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "someField", new AttributeValue().withS("someValue1UpdatedAgain")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner3")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue3"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue3a")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue3"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue3a"),
                        "someField", new AttributeValue().withS("someValue3a")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner3")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue3"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue3b")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue3"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue3b"),
                        "someField", new AttributeValue().withS("someValue3b")))),
            new MtRecord()
                .withContext("ctx1")
                .withTableName(getPrefix() + "MtAmazonDynamoDbTestRunner3")
                .withEventName(INSERT.name())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue")))
                    .withNewImage(ImmutableMap.of("hashKeyField", new AttributeValue().withS("hashKeyValue"),
                        "rangeKeyField", new AttributeValue().withS("rangeKeyValue"),
                        "indexField", new AttributeValue().withS("indexFieldValue"))))
        );
    }

    void runBinaryTest() {
        String testDescription = getAmazonDynamoDbSupplier() + " with hashkey type=B";
        log.info("START test " + testDescription);

        // setup
        setup(ImmutableList.of(
            ImmutableMap.of("ctx1", createTableRequest4),
            ImmutableMap.of("ctx2", createTableRequest4)
        ));

        // test
        getAmazonDynamoDbSupplier().createTable(createTableRequest4);
        testWithHashKeyType(createTableRequest4,
            tableName4,
            value -> new AttributeValue().withB(UTF_8.encode(value))
        );

        // teardown
        teardown();

        log.info("END test " + testDescription);
    }

    /*
     * This test is independent of run() test because run() does not work with table hashkey type of B(binary).
     * Because AttributeType's containing binary content can only be decoded once, we get false assertion failures.
     * This can be fixed with some refactoring.
     */
    private void testWithHashKeyType(CreateTableRequest createTableRequest,
        String tableName,
        Function<String, AttributeValue> attributeValueGeneratorFunction) {
        /*
         * The attributeValueGeneratorFunction is necessary because the ByteBuffer contained in a AttributeType
         * can only be decoded once and thus can't be reused across tests
         */
        final Supplier<AttributeValue> generator1 = () -> attributeValueGeneratorFunction.apply("41");
        final Supplier<AttributeValue> generator2 = () -> attributeValueGeneratorFunction.apply("42");

        getAmazonDynamoDbSupplier().createTable(createTableRequest);
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item41 = createItem(generator1.get(), "someValue41");
        getAmazonDynamoDbSupplier().putItem(new PutItemRequest().withTableName(tableName).withItem(item41));

        // put item in ctx2
        mtContext.setContext("ctx2");
        Map<String, AttributeValue> item42 = createItem(generator2.get(), "someValue42");
        getAmazonDynamoDbSupplier().putItem(new PutItemRequest().withTableName(tableName).withItem(item42));

        // get item from ctx1
        mtContext.setContext("ctx1");
        Map<String, AttributeValue> item41get = getAmazonDynamoDbSupplier()
            .getItem(
                new GetItemRequest().withTableName(tableName).withKey(ImmutableMap.of(hashKeyField, generator1.get())))
            .getItem();
        assertThat(item41get, is(ImmutableMap.of(hashKeyField, generator1.get(),
            someField, new AttributeValue().withS("someValue41"))));

        // get item from ctx2
        mtContext.setContext("ctx2");
        Map<String, AttributeValue> item42get = getAmazonDynamoDbSupplier()
            .getItem(
                new GetItemRequest().withTableName(tableName).withKey(ImmutableMap.of(hashKeyField, generator2.get())))
            .getItem();
        assertThat(item42get, is(ImmutableMap.of(hashKeyField, generator2.get(),
            someField, new AttributeValue().withS("someValue42"))));

        // query item from ctx1
        mtContext.setContext("ctx1");
        List<Map<String, AttributeValue>> item41query = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", generator1.get()))).getItems();
        assertEquals(1, item41query.size());
        assertThat(item41query.get(0), is(ImmutableMap.of(hashKeyField, generator1.get(),
            someField, new AttributeValue().withS("someValue41"))));

        // query item from ctx2
        mtContext.setContext("ctx2");
        List<Map<String, AttributeValue>> item42query = getAmazonDynamoDbSupplier().query(
            new QueryRequest().withTableName(tableName).withKeyConditionExpression("#name = :value")
                .withExpressionAttributeNames(ImmutableMap.of("#name", hashKeyField))
                .withExpressionAttributeValues(ImmutableMap.of(":value", generator2.get()))).getItems();
        assertEquals(1, item42query.size());
        assertThat(item42query.get(0), is(ImmutableMap.of(hashKeyField, generator2.get(),
            someField, new AttributeValue().withS("someValue42"))));
    }

    void teardown() {
        streamTestRunner.stop();
        for (String tableName : rootAmazonDynamoDb.listTables().getTableNames()) {
            String prefix = getPrefix();
            if (tableName.startsWith(prefix)) {
                new AmazonDynamoDbAdminUtils(rootAmazonDynamoDb)
                    .deleteTableIfExists(tableName, getPollInterval(), timeoutSeconds);
            }
        }
    }

    private Map<String, AttributeValue> createItem(AttributeValue hashKeyValue, String someFieldValue) {
        return new HashMap<>(ImmutableMap.of(hashKeyField, hashKeyValue,
            someField, createStringAttribute(someFieldValue)));
    }

    private Map<String, AttributeValue> createItem(String someFieldValue) {
        return createItem(hashKeyField, "hashKeyValue", someField, someFieldValue);
    }

    protected Map<String, AttributeValue> createItem(String hashKeyField, String hashKeyValue, String someField,
        String someFieldValue) {
        return new HashMap<>(ImmutableMap.of(hashKeyField, createAttribute(hashKeyValue),
            someField, createStringAttribute(someFieldValue)));
    }

    private AttributeValue createStringAttribute(String value) {
        return new AttributeValue().withS(value);
    }

    private AttributeValue createAttribute(String value) {
        AttributeValue attr = new AttributeValue();
        switch (hashKeyAttrType) {
            case S:
                attr.setS(value);
                return attr;
            case N:
                attr.setN(value);
                return attr;
            case B:
                attr.setB(UTF_8.encode(value));
                return attr;
            default:
                throw new IllegalArgumentException("unsupported type " + hashKeyAttrType + " encountered");
        }
    }

    private String getValue(AttributeValue attr) {
        switch (hashKeyAttrType) {
            case B:
                return new StringBuffer(UTF_8.decode(attr.getB())).toString();
            case N:
                return attr.getN();
            case S:
                return attr.getS();
            default:
                throw new IllegalArgumentException("unsupported type " + hashKeyAttrType + " encountered");
        }
    }

    private Map<String, AttributeValue> getItem(String tableName) {
        return getItem(tableName, "hashKeyValue");
    }

    private Map<String, AttributeValue> getItem(String tableName, String value) {
        Map<String, AttributeValue> keys = new HashMap<>(ImmutableMap.of(hashKeyField, createAttribute(value)));
        Map<String, AttributeValue> originalKeys = new HashMap<>(ImmutableMap.of(hashKeyField, createAttribute(value)));
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName).withKey(keys);
        GetItemResult getItemResult = getAmazonDynamoDbSupplier().getItem(getItemRequest);
        assertEquals(tableName, getItemRequest.getTableName());
        assertThat(getItemRequest.getKey(), is(originalKeys));
        return getItemResult.getItem();
    }

    private void assertItemValue(String expectedValue, Map<String, AttributeValue> actualItem) {
        assertThat(actualItem, is(ImmutableMap.of(hashKeyField, createAttribute("hashKeyValue"),
            someField, createStringAttribute(expectedValue))));
    }

    protected void deleteTable(String tenantId, String tableName) {
        mtContext.setContext(tenantId);
        new TestAmazonDynamoDbAdminUtils(getAmazonDynamoDbSupplier())
            .deleteTableIfExists(tableName, getPollInterval(), timeoutSeconds);
    }

    protected void createTable(String context, String tableName) {
        mtContext.setContext(context);
        createTable(context, new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, hashKeyAttrType))
            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withTableName(tableName));
    }

    private void createTable(String context, CreateTableRequest createTableRequest) {
        mtContext.setContext(context);
        new TestAmazonDynamoDbAdminUtils(getAmazonDynamoDbSupplier())
            .createTableIfNotExists(createTableRequest, getPollInterval());
    }

    protected void recreateTable(String context, String tableName) {
        mtContext.setContext(context);
        deleteTable(context, tableName);
        createTable(context, tableName);
    }

    private void recreateTable(String context, CreateTableRequest createTableRequest) {
        mtContext.setContext(context);
        deleteTable(context, createTableRequest.getTableName());
        createTable(context, createTableRequest);
    }

    protected int getPollInterval() {
        return isLocalDynamo ? 0 : 1;
    }

    private String buildTableName(int ordinal) {
        return buildTableName(getClass().getSimpleName() + ordinal);
    }

    private String buildTableName(String table) {
        return getPrefix() + table;
    }

    private String getPrefix() {
        return (isLocalDynamo ? "" : "oktodelete-" + TestAmazonDynamoDbAdminUtils.getLocalHost() + "-");
    }

}