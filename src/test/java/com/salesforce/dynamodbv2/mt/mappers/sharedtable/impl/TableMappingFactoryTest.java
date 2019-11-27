/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.amazonaws.services.dynamodbv2.model.DescribeContributorInsightsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeContributorInsightsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeEndpointsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeEndpointsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableReplicaAutoScalingRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableReplicaAutoScalingResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult;
import com.amazonaws.services.dynamodbv2.model.ListContributorInsightsRequest;
import com.amazonaws.services.dynamodbv2.model.ListContributorInsightsResult;
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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupResult;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.UntagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.UntagResourceResult;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateContributorInsightsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateContributorInsightsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableReplicaAutoScalingRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableReplicaAutoScalingResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class TableMappingFactoryTest {

    // can't spy directly, because local DDB uses proxy
    final AmazonDynamoDB dynamoDb = spy(new ForwardingAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal()));

    /**
     * Verifies that TableMappingFactory creates physical table eagerly during construction if requested.
     */
    @Test
    void testCreateEager(TestInfo testInfo) {
        final String tableName = getTestTableName(testInfo);
        final TableMappingFactory sut = createTestFactory(tableName, true);

        // physical table should exist
        assertEquals(TableStatus.ACTIVE.toString(), dynamoDb.describeTable(tableName).getTable().getTableStatus());

        // should not have called describe again
        reset(dynamoDb);
        final DynamoTableDescription description = new DynamoTableDescriptionImpl(simpleTable("vtable"));
        sut.getTableMapping(description);
        verify(dynamoDb, never()).describeTable(any(String.class));
    }

    /**
     * Verifies that table info cached after first use.
     */
    @Test
    void testCreateLazy(TestInfo testInfo) {
        final String tableName = getTestTableName(testInfo);
        final TableMappingFactory sut = createTestFactory(tableName, false);
        // physical table should not have been created
        assertThrows(ResourceNotFoundException.class, () -> dynamoDb.describeTable(tableName));

        // should create table lazily
        final DynamoTableDescription description = new DynamoTableDescriptionImpl(simpleTable("vtable"));
        sut.getTableMapping(description);
        assertEquals(TableStatus.ACTIVE.toString(), dynamoDb.describeTable(tableName).getTable().getTableStatus());

        // subsequent requests should no longer call describe
        reset(dynamoDb);
        sut.getTableMapping(description);
        verify(dynamoDb, never()).describeTable(any(String.class));
    }

    /**
     * Verifies that physical table is described on first access for lazy mode and cached afterwards.
     */
    @Test
    void testExistsLazy(TestInfo testInfo) {
        // simulate creating table in different instance
        final String tableName = getTestTableName(testInfo);
        createTestFactory(tableName, true);
        assertEquals(TableStatus.ACTIVE.toString(), dynamoDb.describeTable(tableName).getTable().getTableStatus());

        // expect that getting a table mapping describes the physical table
        final TableMappingFactory sut = createTestFactory(tableName, false);
        final DynamoTableDescription description = new DynamoTableDescriptionImpl(simpleTable("vtable"));
        sut.getTableMapping(description);
        verify(dynamoDb, atLeastOnce()).describeTable(tableName);

        // subsequent requests should no longer call describe
        reset(dynamoDb);
        sut.getTableMapping(description);
        verify(dynamoDb, never()).describeTable(any(String.class));
    }

    private static String getTestTableName(TestInfo testInfo) {
        return testInfo.getTestClass().get().getSimpleName()
            + '.' + testInfo.getTestMethod().get().getName()
            + '.' + System.currentTimeMillis();
    }

    private TableMappingFactory createTestFactory(String tableName, boolean createEagerly) {
        final TablePartitioningStrategy strategy = mock(TablePartitioningStrategy.class);
        when(strategy.createTableMapping(any(), any(), any(), any())).thenReturn(mock(TableMapping.class));

        return new TableMappingFactory(
            new SingletonCreateTableRequestFactory(simpleTable(tableName)),
            Optional::empty,
            dynamoDb,
            strategy,
            createEagerly,
            0
        );
    }

    private static CreateTableRequest simpleTable(String tableName) {
        return new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("id", ScalarAttributeType.S))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    }

    private static class ForwardingAmazonDynamoDb implements AmazonDynamoDB {
        private final AmazonDynamoDB amazonDynamoDb;

        public ForwardingAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
        }

        @Override
        @Deprecated
        public void setEndpoint(String endpoint) {
            amazonDynamoDb.setEndpoint(endpoint);
        }

        @Override
        @Deprecated
        public void setRegion(Region region) {
            amazonDynamoDb.setRegion(region);
        }

        @Override
        public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
            return amazonDynamoDb.batchGetItem(batchGetItemRequest);
        }

        @Override
        public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems,
                                               String returnConsumedCapacity) {
            return amazonDynamoDb.batchGetItem(requestItems, returnConsumedCapacity);
        }

        @Override
        public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems) {
            return amazonDynamoDb.batchGetItem(requestItems);
        }

        @Override
        public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
            return amazonDynamoDb.batchWriteItem(batchWriteItemRequest);
        }

        @Override
        public BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> requestItems) {
            return amazonDynamoDb.batchWriteItem(requestItems);
        }

        @Override
        public CreateBackupResult createBackup(CreateBackupRequest createBackupRequest) {
            return amazonDynamoDb.createBackup(createBackupRequest);
        }

        @Override
        public CreateGlobalTableResult createGlobalTable(CreateGlobalTableRequest createGlobalTableRequest) {
            return amazonDynamoDb.createGlobalTable(createGlobalTableRequest);
        }

        @Override
        public CreateTableResult createTable(CreateTableRequest createTableRequest) {
            return amazonDynamoDb.createTable(createTableRequest);
        }

        @Override
        public CreateTableResult createTable(List<AttributeDefinition> attributeDefinitions,
                                             String tableName,
                                             List<KeySchemaElement> keySchema,
                                             ProvisionedThroughput provisionedThroughput) {
            return amazonDynamoDb.createTable(attributeDefinitions, tableName, keySchema, provisionedThroughput);
        }

        @Override
        public DeleteBackupResult deleteBackup(DeleteBackupRequest deleteBackupRequest) {
            return amazonDynamoDb.deleteBackup(deleteBackupRequest);
        }

        @Override
        public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
            return amazonDynamoDb.deleteItem(deleteItemRequest);
        }

        @Override
        public DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key) {
            return amazonDynamoDb.deleteItem(tableName, key);
        }

        @Override
        public DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key, String returnValues) {
            return amazonDynamoDb.deleteItem(tableName, key, returnValues);
        }

        @Override
        public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
            return amazonDynamoDb.deleteTable(deleteTableRequest);
        }

        @Override
        public DeleteTableResult deleteTable(String tableName) {
            return amazonDynamoDb.deleteTable(tableName);
        }

        @Override
        public DescribeBackupResult describeBackup(DescribeBackupRequest describeBackupRequest) {
            return amazonDynamoDb.describeBackup(describeBackupRequest);
        }

        @Override
        public DescribeContinuousBackupsResult describeContinuousBackups(
            DescribeContinuousBackupsRequest describeContinuousBackupsRequest) {
            return amazonDynamoDb.describeContinuousBackups(describeContinuousBackupsRequest);
        }

        @Override
        public DescribeContributorInsightsResult describeContributorInsights(
            DescribeContributorInsightsRequest describeContributorInsightsRequest) {
            return amazonDynamoDb.describeContributorInsights(describeContributorInsightsRequest);
        }

        @Override
        public DescribeEndpointsResult describeEndpoints(DescribeEndpointsRequest describeEndpointsRequest) {
            return amazonDynamoDb.describeEndpoints(describeEndpointsRequest);
        }

        @Override
        public DescribeGlobalTableResult describeGlobalTable(DescribeGlobalTableRequest describeGlobalTableRequest) {
            return amazonDynamoDb.describeGlobalTable(describeGlobalTableRequest);
        }

        @Override
        public DescribeGlobalTableSettingsResult describeGlobalTableSettings(
            DescribeGlobalTableSettingsRequest describeGlobalTableSettingsRequest) {
            return amazonDynamoDb.describeGlobalTableSettings(describeGlobalTableSettingsRequest);
        }

        @Override
        public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
            return amazonDynamoDb.describeLimits(describeLimitsRequest);
        }

        @Override
        public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
            return amazonDynamoDb.describeTable(describeTableRequest);
        }

        @Override
        public DescribeTableResult describeTable(String tableName) {
            return amazonDynamoDb.describeTable(tableName);
        }

        @Override
        public DescribeTableReplicaAutoScalingResult describeTableReplicaAutoScaling(
            DescribeTableReplicaAutoScalingRequest describeTableReplicaAutoScalingRequest) {
            return amazonDynamoDb.describeTableReplicaAutoScaling(describeTableReplicaAutoScalingRequest);
        }

        @Override
        public DescribeTimeToLiveResult describeTimeToLive(DescribeTimeToLiveRequest describeTimeToLiveRequest) {
            return amazonDynamoDb.describeTimeToLive(describeTimeToLiveRequest);
        }

        @Override
        public GetItemResult getItem(GetItemRequest getItemRequest) {
            return amazonDynamoDb.getItem(getItemRequest);
        }

        @Override
        public GetItemResult getItem(String tableName, Map<String, AttributeValue> key) {
            return amazonDynamoDb.getItem(tableName, key);
        }

        @Override
        public GetItemResult getItem(String tableName, Map<String, AttributeValue> key, Boolean consistentRead) {
            return amazonDynamoDb.getItem(tableName, key, consistentRead);
        }

        @Override
        public ListBackupsResult listBackups(ListBackupsRequest listBackupsRequest) {
            return amazonDynamoDb.listBackups(listBackupsRequest);
        }

        @Override
        public ListContributorInsightsResult listContributorInsights(
            ListContributorInsightsRequest listContributorInsightsRequest) {
            return amazonDynamoDb.listContributorInsights(listContributorInsightsRequest);
        }

        @Override
        public ListGlobalTablesResult listGlobalTables(ListGlobalTablesRequest listGlobalTablesRequest) {
            return amazonDynamoDb.listGlobalTables(listGlobalTablesRequest);
        }

        @Override
        public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
            return amazonDynamoDb.listTables(listTablesRequest);
        }

        @Override
        public ListTablesResult listTables() {
            return amazonDynamoDb.listTables();
        }

        @Override
        public ListTablesResult listTables(String exclusiveStartTableName) {
            return amazonDynamoDb.listTables(exclusiveStartTableName);
        }

        @Override
        public ListTablesResult listTables(String exclusiveStartTableName, Integer limit) {
            return amazonDynamoDb.listTables(exclusiveStartTableName, limit);
        }

        @Override
        public ListTablesResult listTables(Integer limit) {
            return amazonDynamoDb.listTables(limit);
        }

        @Override
        public ListTagsOfResourceResult listTagsOfResource(ListTagsOfResourceRequest listTagsOfResourceRequest) {
            return amazonDynamoDb.listTagsOfResource(listTagsOfResourceRequest);
        }

        @Override
        public PutItemResult putItem(PutItemRequest putItemRequest) {
            return amazonDynamoDb.putItem(putItemRequest);
        }

        @Override
        public PutItemResult putItem(String tableName, Map<String, AttributeValue> item) {
            return amazonDynamoDb.putItem(tableName, item);
        }

        @Override
        public PutItemResult putItem(String tableName, Map<String, AttributeValue> item, String returnValues) {
            return amazonDynamoDb.putItem(tableName, item, returnValues);
        }

        @Override
        public QueryResult query(QueryRequest queryRequest) {
            return amazonDynamoDb.query(queryRequest);
        }

        @Override
        public RestoreTableFromBackupResult restoreTableFromBackup(
            RestoreTableFromBackupRequest restoreTableFromBackupRequest) {
            return amazonDynamoDb.restoreTableFromBackup(restoreTableFromBackupRequest);
        }

        @Override
        public RestoreTableToPointInTimeResult restoreTableToPointInTime(
            RestoreTableToPointInTimeRequest restoreTableToPointInTimeRequest) {
            return amazonDynamoDb.restoreTableToPointInTime(restoreTableToPointInTimeRequest);
        }

        @Override
        public ScanResult scan(ScanRequest scanRequest) {
            return amazonDynamoDb.scan(scanRequest);
        }

        @Override
        public ScanResult scan(String tableName, List<String> attributesToGet) {
            return amazonDynamoDb.scan(tableName, attributesToGet);
        }

        @Override
        public ScanResult scan(String tableName, Map<String, Condition> scanFilter) {
            return amazonDynamoDb.scan(tableName, scanFilter);
        }

        @Override
        public ScanResult scan(String tableName, List<String> attributesToGet, Map<String, Condition> scanFilter) {
            return amazonDynamoDb.scan(tableName, attributesToGet, scanFilter);
        }

        @Override
        public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
            return amazonDynamoDb.tagResource(tagResourceRequest);
        }

        @Override
        public TransactGetItemsResult transactGetItems(TransactGetItemsRequest transactGetItemsRequest) {
            return amazonDynamoDb.transactGetItems(transactGetItemsRequest);
        }

        @Override
        public TransactWriteItemsResult transactWriteItems(TransactWriteItemsRequest transactWriteItemsRequest) {
            return amazonDynamoDb.transactWriteItems(transactWriteItemsRequest);
        }

        @Override
        public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
            return amazonDynamoDb.untagResource(untagResourceRequest);
        }

        @Override
        public UpdateContinuousBackupsResult updateContinuousBackups(
            UpdateContinuousBackupsRequest updateContinuousBackupsRequest) {
            return amazonDynamoDb.updateContinuousBackups(updateContinuousBackupsRequest);
        }

        @Override
        public UpdateContributorInsightsResult updateContributorInsights(
            UpdateContributorInsightsRequest updateContributorInsightsRequest) {
            return amazonDynamoDb.updateContributorInsights(updateContributorInsightsRequest);
        }

        @Override
        public UpdateGlobalTableResult updateGlobalTable(UpdateGlobalTableRequest updateGlobalTableRequest) {
            return amazonDynamoDb.updateGlobalTable(updateGlobalTableRequest);
        }

        @Override
        public UpdateGlobalTableSettingsResult updateGlobalTableSettings(
            UpdateGlobalTableSettingsRequest updateGlobalTableSettingsRequest) {
            return amazonDynamoDb.updateGlobalTableSettings(updateGlobalTableSettingsRequest);
        }

        @Override
        public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
            return amazonDynamoDb.updateItem(updateItemRequest);
        }

        @Override
        public UpdateItemResult updateItem(String tableName, Map<String, AttributeValue> key,
                                           Map<String, AttributeValueUpdate> attributeUpdates) {
            return amazonDynamoDb.updateItem(tableName, key, attributeUpdates);
        }

        @Override
        public UpdateItemResult updateItem(String tableName, Map<String, AttributeValue> key,
                                           Map<String, AttributeValueUpdate> attributeUpdates, String returnValues) {
            return amazonDynamoDb.updateItem(tableName, key, attributeUpdates, returnValues);
        }

        @Override
        public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
            return amazonDynamoDb.updateTable(updateTableRequest);
        }

        @Override
        public UpdateTableResult updateTable(String tableName, ProvisionedThroughput provisionedThroughput) {
            return amazonDynamoDb.updateTable(tableName, provisionedThroughput);
        }

        @Override
        public UpdateTableReplicaAutoScalingResult updateTableReplicaAutoScaling(
            UpdateTableReplicaAutoScalingRequest updateTableReplicaAutoScalingRequest) {
            return amazonDynamoDb.updateTableReplicaAutoScaling(updateTableReplicaAutoScalingRequest);
        }

        @Override
        public UpdateTimeToLiveResult updateTimeToLive(UpdateTimeToLiveRequest updateTimeToLiveRequest) {
            return amazonDynamoDb.updateTimeToLive(updateTimeToLiveRequest);
        }

        @Override
        public void shutdown() {
            amazonDynamoDb.shutdown();
        }

        @Override
        public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
            return amazonDynamoDb.getCachedResponseMetadata(request);
        }

        @Override
        public AmazonDynamoDBWaiters waiters() {
            return amazonDynamoDb.waiters();
        }
    }
}
