/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import java.util.Optional;

/**
 * Allows for dividing tenants into their own tables by prefixing table names with the multi-tenant context.
 *
 * The multi-tenant context is separated from the table name by the delimiter, which is '.' by default.
 *
 * To use, call the static builder() method.  The following parameters are required ...
 * - an AmazonDynamoDB instance
 * - a multi-tenant context
 *
 * The following are optional arguments ...
 * - delimiter: a String delimiter used to separate the tenant identifier prefix from the table name
 * 
 * Supported: create|describe|delete Table, get|putItem, scan, query
 *
 * @author msgroi
 */
public class MTAmazonDynamoDBByTable extends MTAmazonDynamoDBBase {

    private final String delimiter;
    private final Optional<String> tablePrefix;

    private MTAmazonDynamoDBByTable(MTAmazonDynamoDBContextProvider mtContext,
                                    AmazonDynamoDB amazonDynamoDB,
                                    String delimiter,
                                    Optional<String> tablePrefix) {
        super(mtContext, amazonDynamoDB);
        this.delimiter = delimiter;
        this.tablePrefix = tablePrefix;
    }

    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        createTableRequest = createTableRequest.clone();
        createTableRequest.withTableName(buildPrefixedTablename(createTableRequest.getTableName()));
        return getAmazonDynamoDB().createTable(createTableRequest);
    }

    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        deleteItemRequest = deleteItemRequest.clone();
        deleteItemRequest.withTableName(buildPrefixedTablename(deleteItemRequest.getTableName()));
        return getAmazonDynamoDB().deleteItem(deleteItemRequest);
    }

    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        String virtualTableName = deleteTableRequest.getTableName();
        deleteTableRequest = deleteTableRequest.clone();
        deleteTableRequest.withTableName(buildPrefixedTablename(deleteTableRequest.getTableName()));
        DeleteTableResult deleteTableResult = getAmazonDynamoDB().deleteTable(deleteTableRequest);
        deleteTableResult.getTableDescription().setTableName(virtualTableName);
        return deleteTableResult;
    }

    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        String virtualTableName = describeTableRequest.getTableName();
        describeTableRequest = describeTableRequest.clone();
        describeTableRequest.withTableName(buildPrefixedTablename(describeTableRequest.getTableName()));
        DescribeTableResult describeTableResult = getAmazonDynamoDB().describeTable(describeTableRequest);
        describeTableResult.getTable().setTableName(virtualTableName);
        return describeTableResult;
    }

    public GetItemResult getItem(GetItemRequest getItemRequest) {
        getItemRequest = getItemRequest.clone();
        String prefixedTableName = buildPrefixedTablename(getItemRequest.getTableName());
        getItemRequest.withTableName(prefixedTableName);
        return getAmazonDynamoDB().getItem(getItemRequest);
    }

    public PutItemResult putItem(PutItemRequest putItemRequest) {
        putItemRequest = putItemRequest.clone();
        putItemRequest.withTableName(buildPrefixedTablename(putItemRequest.getTableName()));
        return getAmazonDynamoDB().putItem(putItemRequest);
    }

    public QueryResult query(QueryRequest queryRequest) {
        queryRequest = queryRequest.clone();
        queryRequest.withTableName(buildPrefixedTablename(queryRequest.getTableName()));
        return getAmazonDynamoDB().query(queryRequest);
    }

    public ScanResult scan(ScanRequest scanRequest) {
        scanRequest = scanRequest.clone();
        scanRequest.withTableName(buildPrefixedTablename(scanRequest.getTableName()));
        return getAmazonDynamoDB().scan(scanRequest);
    }

    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        updateItemRequest = updateItemRequest.clone();
        updateItemRequest.withTableName(buildPrefixedTablename(updateItemRequest.getTableName()));
        return getAmazonDynamoDB().updateItem(updateItemRequest);
    }

    public static MTAmazonDynamoDBBuilder builder() {
        return new MTAmazonDynamoDBBuilder();
    }

    public static class MTAmazonDynamoDBBuilder {

        private AmazonDynamoDB amazonDynamoDB;
        private MTAmazonDynamoDBContextProvider mtContext;
        private String delimiter;
        private Optional<String> tablePrefix;

        public MTAmazonDynamoDBBuilder withAmazonDynamoDB(AmazonDynamoDB amazonDynamoDB) {
            this.amazonDynamoDB = amazonDynamoDB;
            return this;
        }

        public MTAmazonDynamoDBBuilder withContext(MTAmazonDynamoDBContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MTAmazonDynamoDBBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public MTAmazonDynamoDBBuilder withTablePrefix(String tablePrefix) {
            this.tablePrefix = Optional.of(tablePrefix);
            return this;
        }

        public MTAmazonDynamoDBByTable build() {
            setDefaults();
            Preconditions.checkNotNull(amazonDynamoDB, "amazonDynamoDB is required");
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            return new MTAmazonDynamoDBByTable(mtContext, amazonDynamoDB, delimiter, tablePrefix);
        }

        private void setDefaults() {
            if (delimiter == null) {
                delimiter = ".";
            }
            if (tablePrefix == null) {
                tablePrefix = Optional.empty();
            }
        }

    }

    @VisibleForTesting
    String buildPrefixedTablename(String virtualTablename) {
        return (tablePrefix.orElse("")) +
               getMTContext().getContext() +
               delimiter + virtualTablename;
    }

}