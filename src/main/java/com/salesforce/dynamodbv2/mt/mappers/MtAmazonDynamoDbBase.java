/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest;
import com.amazonaws.services.dynamodbv2.model.CreateBackupResult;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteBackupRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteBackupResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeBackupRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeBackupResult;
import com.amazonaws.services.dynamodbv2.model.DescribeContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeContinuousBackupsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult;
import com.amazonaws.services.dynamodbv2.model.ListGlobalTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListGlobalTablesResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupResult;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.model.UntagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.UntagResourceResult;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for each mapping scheme to extend.  It reduces code by ...
 * - throwing an UnsupportedOperationException for each unsupported method
 * - providing pass-through to an AmazonDynamoDB and MtAmazonDynamoDbContextProvider passed into the constructor
 * - providing the ability to override the method that returns said AmazonDynamoDB
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbBase implements MtAmazonDynamoDb {

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final AmazonDynamoDB amazonDynamoDb;

    public MtAmazonDynamoDbBase(MtAmazonDynamoDbContextProvider mtContext,
                                AmazonDynamoDB amazonDynamoDb) {
        this.mtContext = mtContext;
        this.amazonDynamoDb = amazonDynamoDb;
    }

    public AmazonDynamoDB getAmazonDynamoDb() {
        return amazonDynamoDb;
    }

    protected MtAmazonDynamoDbContextProvider getMtContext() {
        return mtContext;
    }

    @Override
    @Deprecated
    public void setEndpoint(String endpoint) {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    @Deprecated
    public void setRegion(Region region) {
        throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        return getAmazonDynamoDb().batchGetItem(batchGetItemRequest);
    }

    @Override
    public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems, String returnConsumedCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> requestItems) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CreateBackupResult createBackup(CreateBackupRequest createBackupRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CreateGlobalTableResult createGlobalTable(CreateGlobalTableRequest createGlobalTableRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        return getAmazonDynamoDb().createTable(createTableRequest);
    }

    @Override
    public CreateTableResult createTable(List<AttributeDefinition> attributeDefinitions,
                                         String tableName,
                                         List<KeySchemaElement> keySchema,
                                         ProvisionedThroughput provisionedThroughput) {
        return createTable(new CreateTableRequest()
            .withAttributeDefinitions(attributeDefinitions)
            .withTableName(tableName)
            .withKeySchema(keySchema)
            .withProvisionedThroughput(provisionedThroughput));
    }

    @Override
    public DeleteBackupResult deleteBackup(DeleteBackupRequest deleteBackupRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        return getAmazonDynamoDb().deleteItem(deleteItemRequest);
    }

    @Override
    public DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key));
    }

    @Override
    public DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key, String returnValues) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key).withReturnValues(returnValues));
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        return getAmazonDynamoDb().deleteTable(deleteTableRequest);
    }

    @Override
    public DeleteTableResult deleteTable(String tableName) {
        return deleteTable(new DeleteTableRequest().withTableName(tableName));
    }

    @Override
    public DescribeBackupResult describeBackup(DescribeBackupRequest describeBackupRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeContinuousBackupsResult describeContinuousBackups(
        DescribeContinuousBackupsRequest describeContinuousBackupsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeGlobalTableResult describeGlobalTable(DescribeGlobalTableRequest describeGlobalTableRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        return getAmazonDynamoDb().describeTable(describeTableRequest);
    }

    @Override
    public DescribeTableResult describeTable(String tableName) {
        return describeTable(new DescribeTableRequest().withTableName(tableName));
    }

    @Override
    public DescribeTimeToLiveResult describeTimeToLive(DescribeTimeToLiveRequest describeTimeToLiveRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        return getAmazonDynamoDb().getItem(getItemRequest);
    }

    @Override
    public GetItemResult getItem(String tableName, Map<String, AttributeValue> key) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key));
    }

    @Override
    public GetItemResult getItem(String tableName, Map<String, AttributeValue> key, Boolean consistentRead) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key).withConsistentRead(consistentRead));
    }

    @Override
    public ListBackupsResult listBackups(ListBackupsRequest listBackupsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListGlobalTablesResult listGlobalTables(ListGlobalTablesRequest listGlobalTablesRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeGlobalTableSettingsResult describeGlobalTableSettings(
        DescribeGlobalTableSettingsRequest describeGlobalTableSettingsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateGlobalTableSettingsResult updateGlobalTableSettings(
        UpdateGlobalTableSettingsRequest updateGlobalTableSettingsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RestoreTableToPointInTimeResult restoreTableToPointInTime(
        RestoreTableToPointInTimeRequest restoreTableToPointInTimeRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateContinuousBackupsResult updateContinuousBackups(
        UpdateContinuousBackupsRequest updateContinuousBackupsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
        throw new UnsupportedOperationException();
    }

    public ListTablesResult listTables() {
        return getAmazonDynamoDb().listTables();
    }

    public ListTablesResult listTables(String exclusiveStartTableName) {
        throw new UnsupportedOperationException();
    }

    public ListTablesResult listTables(String exclusiveStartTableName, Integer limit) {
        throw new UnsupportedOperationException();
    }

    public ListTablesResult listTables(Integer limit) {
        throw new UnsupportedOperationException();
    }

    List<String> listAllTables() {
        List<String> tables = new ArrayList<>();
        ListTablesResult result;
        String lastEvaluated = null;//Below loop is to iterate through pages
        do {
            result = (lastEvaluated == null)
                ? getAmazonDynamoDb().listTables()
                : getAmazonDynamoDb().listTables(lastEvaluated);
            if (result != null) {
                tables.addAll(result.getTableNames());
                lastEvaluated = result.getLastEvaluatedTableName();
            }
        } while (lastEvaluated != null);

        return tables;
    }

    public ListTagsOfResourceResult listTagsOfResource(ListTagsOfResourceRequest listTagsOfResourceRequest) {
        throw new UnsupportedOperationException();
    }

    public PutItemResult putItem(PutItemRequest putItemRequest) {
        return getAmazonDynamoDb().putItem(putItemRequest);
    }

    public PutItemResult putItem(String tableName, Map<String, AttributeValue> item) {
        return putItem(new PutItemRequest().withTableName(tableName).withItem(item));
    }

    public PutItemResult putItem(String tableName, Map<String, AttributeValue> item, String returnValues) {
        return putItem(new PutItemRequest().withTableName(tableName).withItem(item).withReturnValues(returnValues));
    }

    public QueryResult query(QueryRequest queryRequest) {
        return getAmazonDynamoDb().query(queryRequest);
    }

    public RestoreTableFromBackupResult restoreTableFromBackup(
        RestoreTableFromBackupRequest restoreTableFromBackupRequest) {
        throw new UnsupportedOperationException();
    }

    public ScanResult scan(ScanRequest scanRequest) {
        return getAmazonDynamoDb().scan(scanRequest);
    }

    @Override
    public ScanResult scan(String tableName, List<String> attributesToGet) {
        return scan(new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet));
    }

    @Override
    public ScanResult scan(String tableName, Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName).withScanFilter(scanFilter));
    }

    @Override
    public ScanResult scan(String tableName, List<String> attributesToGet, Map<String, Condition> scanFilter) {
        return scan(new ScanRequest().withTableName(tableName)
            .withAttributesToGet(attributesToGet)
            .withScanFilter(scanFilter));
    }

    @Override
    public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateGlobalTableResult updateGlobalTable(UpdateGlobalTableRequest updateGlobalTableRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        return getAmazonDynamoDb().updateItem(updateItemRequest);
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
                                       Map<String, AttributeValue> key,
                                       Map<String, AttributeValueUpdate> attributeUpdates) {
        return updateItem(new UpdateItemRequest().withTableName(tableName)
            .withKey(key)
            .withAttributeUpdates(attributeUpdates));
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
                                       Map<String, AttributeValue> key,
                                       Map<String, AttributeValueUpdate> attributeUpdates,
                                       String returnValues) {
        return updateItem(new UpdateItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withAttributeUpdates(attributeUpdates)
            .withReturnValues(returnValues));
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateTableResult updateTable(String tableName, ProvisionedThroughput provisionedThroughput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UpdateTimeToLiveResult updateTimeToLive(UpdateTimeToLiveRequest updateTimeToLiveRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AmazonDynamoDBWaiters waiters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MtStreamDescription> listStreams(IRecordProcessorFactory factory) {
        AmazonDynamoDB dynamo = getAmazonDynamoDb();
        if (dynamo instanceof MtAmazonDynamoDb) {
            return ((MtAmazonDynamoDb) getAmazonDynamoDb()).listStreams(factory);
        }
        throw new UnsupportedOperationException();
    }

}