package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.getKeyFromItem;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.projectionContainsKey;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.validateUpdateItemRequest;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.DeleteItemRequestWrapper;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.PutItemRequestWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps virtual table requests to underlying "physical" {@link AmazonDynamoDB} dynamoDB instance.
 */
public class SharedTableMappingDelegate {

    private final AmazonDynamoDB dynamoDB;

    public SharedTableMappingDelegate(AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    // TODO inline?
    public AmazonDynamoDB getDynamoDB() {
        return dynamoDB;
    }

    public PutItemResult putItem(TableMapping mapping, PutItemRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // map conditions
        mapping.getConditionMapper().applyToFilterExpression(new PutItemRequestWrapper(request));

        // map item
        request.setItem(mapping.getItemMapper().applyForWrite(request.getItem()));

        // put
        return getDynamoDB().putItem(request);
    }

    public GetItemResult getItem(TableMapping mapping, GetItemRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // map key
        request.setKey(mapping.getItemMapper().applyToKeyAttributes(request.getKey(), null));

        // get
        GetItemResult getItemResult = getDynamoDB().getItem(request);

        // map result
        if (getItemResult.getItem() != null) {
            getItemResult.withItem(mapping.getItemMapper().reverse(getItemResult.getItem()));
        }

        return getItemResult;
    }

    public UpdateItemResult updateItem(TableMapping mapping, UpdateItemRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        // validate that attributeUpdates are not being used
        validateUpdateItemRequest(request);

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // map key
        request.setKey(mapping.getItemMapper().applyToKeyAttributes(request.getKey(), null));

        // map conditions
        mapping.getConditionMapper().applyForUpdate(request);

        // update
        return getDynamoDB().updateItem(request);
    }

    public QueryResult query(TableMapping mapping, QueryRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // map query request
        mapping.getQueryAndScanMapper().apply(request);

        // map result
        final QueryResult queryResult = getDynamoDB().query(request);
        queryResult.setItems(queryResult.getItems().stream().map(mapping.getItemMapper()::reverse)
            .collect(toList()));
        if (queryResult.getLastEvaluatedKey() != null) {
            queryResult.setLastEvaluatedKey(mapping.getItemMapper().reverse(queryResult.getLastEvaluatedKey()));
        }

        return queryResult;
    }

    public ScanResult scan(TableMapping mapping, ScanRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        PrimaryKey key = request.getIndexName() == null ? mapping.getVirtualTable().getPrimaryKey()
            : mapping.getVirtualTable().findSi(request.getIndexName()).getPrimaryKey();

        // Projection must include primary key, since we use it for paging.
        // (We could add key fields into projection and filter result in the future)
        checkArgument(projectionContainsKey(request, key),
            "Multitenant scans must include key in projection expression");

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // execute scan, keep moving forward pages until we find at least one record for current tenant or reach end
        ScanResult scanResult = mapping.getQueryAndScanMapper().executeScan(getDynamoDB(), request);

        // map result
        List<Map<String, AttributeValue>> items = scanResult.getItems();
        if (!items.isEmpty()) {
            scanResult.setItems(items.stream().map(mapping.getItemMapper()::reverse).collect(toList()));
            if (scanResult.getLastEvaluatedKey() != null) {
                Map<String, AttributeValue> lastItem = Iterables.getLast(scanResult.getItems());
                Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
                lastEvaluatedKey.putAll(getKeyFromItem(lastItem, mapping.getVirtualTable().getPrimaryKey()));
                if (request.getIndexName() != null) {
                    lastEvaluatedKey.putAll(getKeyFromItem(lastItem, key));
                }
                scanResult.setLastEvaluatedKey(lastEvaluatedKey);
            }
        } // else: while loop ensures that getLastEvaluatedKey is null (no need to map)

        return scanResult;
    }

    public BatchGetItemResult batchGetItem(TableMapping mapping, BatchGetItemRequest virtualRequest) {
        checkArgument(virtualRequest.getRequestItems()
            .keySet().size() == 1);
        checkArgument(virtualRequest.getRequestItems()
            .keySet().contains(mapping.getVirtualTable().getTableName()));
        virtualRequest.getRequestItems().values()
            .forEach(MtAmazonDynamoDbBySharedTable::validateGetItemKeysAndAttribute);

        // clone request and clear items
        BatchGetItemRequest physicalRequest = virtualRequest.clone();
        physicalRequest.clearRequestItemsEntries();

        // map keys
        KeysAndAttributes virtualKeys = virtualRequest
            .getRequestItems()
            .get(mapping.getVirtualTable().getTableName());

        physicalRequest.addRequestItemsEntry(
            mapping.getPhysicalTable().getTableName(),
            new KeysAndAttributes().withKeys(virtualKeys.getKeys().stream()
                .map(key -> mapping.getItemMapper().applyToKeyAttributes(key, null))
                .collect(Collectors.toList())));

        // batch get
        final BatchGetItemResult physicalResult = getDynamoDB()
            .batchGetItem(physicalRequest);
        Map<String, List<Map<String, AttributeValue>>> qualifiedItemsByTable = physicalResult
            .getResponses();

        // map result
        final BatchGetItemResult virtualResult = physicalResult.clone();
        virtualResult.clearResponsesEntries();

        List<Map<String, AttributeValue>> qualifiedItems = physicalResult
            .getResponses()
            .get(mapping.getPhysicalTable().getTableName());
        virtualResult.addResponsesEntry(
            mapping.getVirtualTable().getTableName(),
            qualifiedItems.stream().map(keysAndAttributes ->
                mapping.getItemMapper().reverse(keysAndAttributes)).collect(Collectors.toList()));

        // map unprocessedKeys
        if (!physicalResult.getUnprocessedKeys().isEmpty()) {
            virtualResult.clearUnprocessedKeysEntries();
            KeysAndAttributes physicalUnprocessedKeys = physicalResult
                .getUnprocessedKeys()
                .get(mapping.getPhysicalTable().getTableName());
            List<Map<String, AttributeValue>> virtualUnprocessedKeys = physicalUnprocessedKeys.getKeys().stream()
                .map(keysAndAttributes -> mapping.getItemMapper().reverse(keysAndAttributes))
                .collect(Collectors.toList());
            virtualResult.addUnprocessedKeysEntry(
                mapping.getVirtualTable().getTableName(),
                new KeysAndAttributes()
                    .withConsistentRead(physicalUnprocessedKeys.getConsistentRead())
                    .withKeys(virtualUnprocessedKeys));
        }

        return virtualResult;
    }

    public DeleteItemResult deleteItem(TableMapping mapping, DeleteItemRequest request) {
        checkArgument(mapping.getVirtualTable().getTableName().equals(request.getTableName()));

        request = request.clone();

        // map table name
        request.withTableName(mapping.getPhysicalTable().getTableName());

        // map key
        request.setKey(mapping.getItemMapper().applyToKeyAttributes(request.getKey(), null));

        // map conditions
        mapping.getConditionMapper().applyToFilterExpression(new DeleteItemRequestWrapper(request));

        // delete
        return getDynamoDB().deleteItem(request);
    }

    public long truncateTable(TableMapping mapping) {
        long deletedCount = 0;
        String tableName = mapping.getVirtualTable().getTableName();
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
        while (true) {
            // TODO reimplement scan and delete in physical namespace
            //      maping to logical and back to physical is unnecessary overhead
            ScanResult scanResult = scan(mapping, scanRequest);
            for (Map<String, AttributeValue> item : scanResult.getItems()) {
                DeleteItemRequest deleteRequest = new DeleteItemRequest()
                    .withTableName(tableName)
                    .withKey(getKeyFromItem(item, mapping.getVirtualTable().getPrimaryKey()));
                deleteItem(mapping, deleteRequest);
            }
            deletedCount += scanResult.getItems().size();
            if (scanResult.getLastEvaluatedKey() == null) {
                break;
            }
            scanRequest.withExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
        return deletedCount;
    }
}
