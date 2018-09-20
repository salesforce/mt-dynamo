/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs all calls.
 *
 * <p>Supported: batchGet|get|put|updateItem, create|delete|describeTable, scan, query
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbLogger extends MtAmazonDynamoDbBase {

    private static final Logger log = LoggerFactory.getLogger(MtAmazonDynamoDbLogger.class);
    private final List<String> methodsToLog;
    private final Optional<Consumer<List<String>>> logCallback;
    private final boolean logAll;

    private MtAmazonDynamoDbLogger(MtAmazonDynamoDbContextProvider mtContext,
                                   AmazonDynamoDB amazonDynamoDb,
                                   Consumer<List<String>> logCallback,
                                   List<String> methodsToLog,
                                   boolean logAll) {
        super(mtContext, amazonDynamoDb);
        this.logCallback = Optional.ofNullable(logCallback);
        this.methodsToLog = methodsToLog;
        this.logAll = logAll;
    }

    /**
     * TODO: write Javadoc.
     */
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        final String tablesAsString = batchGetItemRequest.getRequestItems().keySet().stream().map(this::tableToString)
                .collect(Collectors.joining(",", "[", "]"));
        log("batchGetItem", tablesAsString, batchGetItemRequest.toString());
        return super.batchGetItem(batchGetItemRequest);
    }

    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        log("createTable", tableToString(createTableRequest.getTableName()), createTableRequest.toString());
        return super.createTable(createTableRequest);
    }

    /**
     * deleteItem logging wrapper.
     */
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        log("deleteItem", tableToString(deleteItemRequest.getTableName()),
                deleteItemRequestToString(deleteItemRequest));
        return super.deleteItem(deleteItemRequest);
    }

    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        log("deleteTable", tableToString(deleteTableRequest.getTableName()));
        return super.deleteTable(deleteTableRequest);
    }

    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        log("describeTable", tableToString(describeTableRequest.getTableName()));
        return super.describeTable(describeTableRequest);
    }

    public GetItemResult getItem(GetItemRequest getItemRequest) {
        log("getItem", tableToString(getItemRequest.getTableName()), keyToString(getItemRequest.getKey()));
        return super.getItem(getItemRequest);
    }

    public PutItemResult putItem(PutItemRequest putItemRequest) {
        log("putItem", tableToString(putItemRequest.getTableName()), itemToString(putItemRequest.getItem()));
        return super.putItem(putItemRequest);
    }

    public QueryResult query(QueryRequest queryRequest) {
        log("query", tableToString(queryRequest.getTableName()), queryRequestToString(queryRequest));
        return super.query(queryRequest);
    }

    public ScanResult scan(ScanRequest scanRequest) {
        log("scan", tableToString(scanRequest.getTableName()), scanRequestToString(scanRequest));
        return super.scan(scanRequest);
    }

    /**
     * updateItem logging wrapper.
     */
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        log("updateItem", tableToString(updateItemRequest.getTableName()),
                updateItemRequestToString(updateItemRequest));
        return super.updateItem(updateItemRequest);
    }

    public static MtAmazonDynamoDbBuilder builder() {
        return new MtAmazonDynamoDbBuilder();
    }

    public static class MtAmazonDynamoDbBuilder {

        private AmazonDynamoDB amazonDynamoDb;
        private MtAmazonDynamoDbContextProvider mtContext;
        private Consumer<List<String>> logCallback;
        private List<String> methodsToLog = new ArrayList<>();
        private boolean logAll;

        public MtAmazonDynamoDbBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
            return this;
        }

        public MtAmazonDynamoDbBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MtAmazonDynamoDbBuilder withLogCallback(Consumer<List<String>> logCallback) {
            this.logCallback = logCallback;
            return this;
        }

        public MtAmazonDynamoDbBuilder withMethodsToLog(List<String> methodsToLog) {
            this.methodsToLog = methodsToLog;
            return this;
        }

        public MtAmazonDynamoDbBuilder withLogAll() {
            this.logAll = true;
            return this;
        }

        /**
         * TODO: write Javadoc.
         *
         * @return a newly created {@code MtAmazonDynamoDbLogger} based on the contents of the
         *     {@code MtAmazonDynamoDbBuilder}
         */
        public MtAmazonDynamoDbLogger build() {
            Preconditions.checkNotNull(amazonDynamoDb, "amazonDynamoDb is required");
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            return new MtAmazonDynamoDbLogger(mtContext, amazonDynamoDb, logCallback, methodsToLog, logAll);
        }

    }

    private String tableToString(String tableName) {
        return "table=" + tableName;
    }

    private String keyToString(Map<String, AttributeValue> key) {
        return "key=" + key;
    }

    private String itemToString(Map<String, AttributeValue> item) {
        return "item=" + item;
    }

    private String queryRequestToString(QueryRequest queryRequest) {
        return "keyConditionExpression=" + queryRequest.getKeyConditionExpression()
            + (queryRequest.getFilterExpression() != null
            ? ", filterExpression=" + queryRequest.getFilterExpression()
            : "")
            + ", names=" + queryRequest.getExpressionAttributeNames()
            + ", values=" + queryRequest.getExpressionAttributeValues()
            + (queryRequest.getIndexName() != null ? ", index=" + queryRequest.getIndexName() : "");
    }

    private String scanRequestToString(ScanRequest scanRequest) {
        return "filterExpression=" + scanRequest.getFilterExpression()
            + ", names=" + scanRequest.getExpressionAttributeNames()
            + ", values=" + scanRequest.getExpressionAttributeValues();
    }

    private String updateItemRequestToString(UpdateItemRequest updateRequest) {
        return (updateRequest.getUpdateExpression() != null
                ? ", updateExpression=" + updateRequest.getUpdateExpression()
                : "")
            + (updateRequest.getAttributeUpdates() != null
                ? ", attributeUpdates=" + updateRequest.getAttributeUpdates()
                : "")
            + ", key=" + updateRequest.getKey()
            + (updateRequest.getConditionExpression() != null
                ? ", conditionExpression=" + updateRequest.getConditionExpression()
                : "")
            + (updateRequest.getExpressionAttributeNames() != null
                ? ", names=" + updateRequest.getExpressionAttributeNames()
                : "")
            + (updateRequest.getExpressionAttributeValues() != null
                ? ", values=" + updateRequest.getExpressionAttributeValues()
                : "");
    }

    private String deleteItemRequestToString(DeleteItemRequest deleteItemRequest) {
        return "key=" + deleteItemRequest.getKey()
            + (deleteItemRequest.getConditionExpression() != null
            ? ", conditionExpression=" + deleteItemRequest.getConditionExpression()
            : "")
            + (deleteItemRequest.getExpressionAttributeNames() != null
            ? ", names=" + deleteItemRequest.getExpressionAttributeNames()
            : "")
            + (deleteItemRequest.getExpressionAttributeValues() != null
            ? ", values=" + deleteItemRequest.getExpressionAttributeValues()
            : "");
    }

    private void log(String method, String... messages) {
        if (logAll || methodsToLog.contains(method)) {
            String concatenatedMessage = "method=" + method + "(), " + Joiner.on(", ").join(messages);
            if (logCallback.isPresent()) {
                logCallback.get().accept(ImmutableList.of(concatenatedMessage));
            } else {
                log.info(concatenatedMessage);
            }
        }
    }

}