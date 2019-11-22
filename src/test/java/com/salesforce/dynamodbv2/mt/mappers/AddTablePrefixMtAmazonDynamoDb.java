/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class AddTablePrefixMtAmazonDynamoDb extends MtAmazonDynamoDbBase {

    private final MtAmazonDynamoDbBase delegate;
    private final String prefix;
    private final Set<String> tableNamesToPrefix;

    public AddTablePrefixMtAmazonDynamoDb(MtAmazonDynamoDbBase delegate,
                                          String prefix,
                                          Set<String> tableNamesToPrefix) {
        super(delegate.getMtContext(), delegate.getAmazonDynamoDb(), delegate.getMeterRegistry(),
            delegate.getScanTenantKey(), delegate.getScanVirtualTableKey());
        this.delegate = delegate;
        this.prefix = prefix;
        this.tableNamesToPrefix = tableNamesToPrefix;
    }

    private String addPrefixIfNeeded(String tableName) {
        return tableNamesToPrefix.contains(tableName) ? prefix + tableName : tableName;
    }

    private String stripPrefixIfNeeded(String tableName) {
        return tableName != null && tableName.startsWith(prefix) ? tableName.substring(prefix.length()) : tableName;
    }

    @Override
    protected boolean isPhysicalTable(String tableName) {
        return delegate.isPhysicalTable(addPrefixIfNeeded(tableName));
    }

    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        batchGetItemRequest = batchGetItemRequest.clone();
        batchGetItemRequest.withRequestItems(
            convertTableNameKeys(batchGetItemRequest.getRequestItems(), this::addPrefixIfNeeded));
        BatchGetItemResult result = delegate.batchGetItem(batchGetItemRequest);
        return result.withResponses(convertTableNameKeys(result.getResponses(), this::stripPrefixIfNeeded));
    }

    private <T> Map<String, T> convertTableNameKeys(Map<String, T> originalMap, UnaryOperator<String> converter) {
        Map<String, T> newMap = new HashMap<>();
        originalMap.forEach((tableName, value) -> newMap.put(converter.apply(tableName), value));
        return newMap;
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        return delegate.createTable(
            createTableRequest.clone().withTableName(addPrefixIfNeeded(createTableRequest.getTableName())));
    }

    @Override
    public CreateTableResult createMultitenantTable(CreateTableRequest createTableRequest) {
        return delegate.createMultitenantTable(
            createTableRequest.clone().withTableName(addPrefixIfNeeded(createTableRequest.getTableName())));
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        return delegate.deleteItem(
            deleteItemRequest.clone().withTableName(addPrefixIfNeeded(deleteItemRequest.getTableName())));
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        return delegate.deleteTable(
            deleteTableRequest.clone().withTableName(addPrefixIfNeeded(deleteTableRequest.getTableName())));
    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        DescribeTableResult result = delegate.describeTable(
            describeTableRequest.clone().withTableName(addPrefixIfNeeded(describeTableRequest.getTableName())));
        String resultTableName = stripPrefixIfNeeded(result.getTable().getTableName());
        return result.withTable(result.getTable().withTableName(resultTableName));
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        return delegate.getItem(
            getItemRequest.clone().withTableName(addPrefixIfNeeded(getItemRequest.getTableName())));
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest request) {
        ListTablesResult result = delegate.listTables(request.getExclusiveStartTableName(), request.getLimit());
        return result.withLastEvaluatedTableName(stripPrefixIfNeeded(result.getLastEvaluatedTableName()))
            .withTableNames(result.getTableNames().stream().map(this::stripPrefixIfNeeded).collect(Collectors.toSet()));
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        return delegate.putItem(
            putItemRequest.clone().withTableName(addPrefixIfNeeded(putItemRequest.getTableName())));
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        return delegate.query(
            queryRequest.clone().withTableName(addPrefixIfNeeded(queryRequest.getTableName())));
    }

    @Override
    public ScanResult scan(ScanRequest scanRequest) {
        return delegate.scan(
            scanRequest.clone().withTableName(addPrefixIfNeeded(scanRequest.getTableName())));
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        return delegate.updateItem(
            updateItemRequest.clone().withTableName(addPrefixIfNeeded(updateItemRequest.getTableName())));
    }
}
