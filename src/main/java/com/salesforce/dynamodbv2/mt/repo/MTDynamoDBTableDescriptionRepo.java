/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDBAdminUtils;
import com.salesforce.dynamodbv2.mt.cache.MTCache;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/*
 * Stores table definitions in single table.  Each record represents a table.  Table names are prefixed with context.
 *
 * The AmazonDynamoDB that it uses must not, itself, be a MTAmazonDynamoDB* instance.  MTAmazonDynamoDBLogger is supported.
 *
 * @author msgroi
 */
public class MTDynamoDBTableDescriptionRepo implements MTTableDescriptionRepo {

    private static final String TABLEMETADATA_HKFIELD = "table";
    private static final String TABLEMETADATA_DATAFIELD = "data";
    private static final String DELIMITER = ".";

    private static final Gson gson = new Gson();
    private final AmazonDynamoDB amazonDynamoDB;
    private final MTAmazonDynamoDBContextProvider mtContext;
    private final AmazonDynamoDBAdminUtils adminUtils;
    private final String tableDescriptionTableName;
    private final String tableDescriptionTableHashKeyField;
    private final String tableDescriptionTableDataField;
    private final String delimiter;
    private final int pollIntervalSeconds;
    private final MTCache<TableDescription> cache;

    private MTDynamoDBTableDescriptionRepo(AmazonDynamoDB amazonDynamoDB,
                                           MTAmazonDynamoDBContextProvider mtContext,
                                           String tableDescriptionTableName,
                                           Optional<String> tablePrefix,
                                           String tableDescriptionTableHashKeyField,
                                           String tableDescriptionTableDataField,
                                           String delimiter,
                                           int pollIntervalSeconds) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.mtContext = mtContext;
        adminUtils = new AmazonDynamoDBAdminUtils(amazonDynamoDB);
        this.tableDescriptionTableName = prefix(tableDescriptionTableName, tablePrefix);
        this.tableDescriptionTableHashKeyField = tableDescriptionTableHashKeyField;
        this.tableDescriptionTableDataField = tableDescriptionTableDataField;
        this.delimiter = delimiter;
        this.pollIntervalSeconds = pollIntervalSeconds;
        cache = new MTCache<>(mtContext);
    }

    @Override
    public TableDescription createTable(CreateTableRequest createTableRequest) {
        amazonDynamoDB.putItem(new PutItemRequest().withTableName(getTableDescriptionTableName()).withItem(createItem(createTableRequest)));
        return getTableDescription(createTableRequest.getTableName());
    }

    @Override
    public TableDescription getTableDescription(String tableName) {
        return getTableDescriptionFromCache(tableName);
    }

    public static MTDynamoDBTableDescriptionRepoBuilder builder() {
        return new MTDynamoDBTableDescriptionRepoBuilder();
    }

    private TableDescription getTableDescriptionFromCache(String tableName) throws ResourceNotFoundException {
        try {
            return cache.get(tableName, () -> getTableDescriptionNoCache(tableName));
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) e.getCause();
            } else {
                throw e;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private TableDescription getTableDescriptionNoCache(String tableName) {
        Map<String, AttributeValue> item = amazonDynamoDB.getItem(new GetItemRequest()
                .withTableName(getTableDescriptionTableName())
                .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField, new AttributeValue(addPrefix(tableName)))))).getItem();
        if (item == null) {
            throw new ResourceNotFoundException("table metadata entry for '" + tableName + "' does not exist in " + tableDescriptionTableName);
        }
        String tableDataJson = item.get(tableDescriptionTableDataField).getS();
        return jsonToTableData(tableDataJson);
    }

    @Override
    public TableDescription deleteTable(String tableName) {
        TableDescription tableDescription = getTableDescription(tableName);

        cache.invalidate(tableName);

        amazonDynamoDB.deleteItem(new DeleteItemRequest()
                .withTableName(getTableDescriptionTableName())
                .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField, new AttributeValue(addPrefix(tableName))))));

        return tableDescription;
    }

    private String getTableDescriptionTableName() {
        try {
            cache.get(tableDescriptionTableName, () -> {
                createTableDescriptionTableIfNotExists(pollIntervalSeconds);
                return new TableDescription().withTableName(tableDescriptionTableName);
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return tableDescriptionTableName;
    }

    private void createTableDescriptionTableIfNotExists(int pollIntervalSeconds) {
        adminUtils.createTableIfNotExists(
                new CreateTableRequest().withTableName(tableDescriptionTableName)
                                        .withKeySchema(new KeySchemaElement().withAttributeName(tableDescriptionTableHashKeyField)
                                                                             .withKeyType(KeyType.HASH))
                                        .withAttributeDefinitions(new AttributeDefinition().withAttributeName(tableDescriptionTableHashKeyField).withAttributeType(ScalarAttributeType.S))
                                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
                pollIntervalSeconds);
    }

    private Map<String, AttributeValue> createItem(CreateTableRequest createTableRequest) {
        TableDescription tableDescription = new TableDescription()
                .withTableName(createTableRequest.getTableName())
                .withKeySchema(createTableRequest.getKeySchema())
                .withAttributeDefinitions(createTableRequest.getAttributeDefinitions())
                .withProvisionedThroughput(new ProvisionedThroughputDescription().withReadCapacityUnits(createTableRequest.getProvisionedThroughput().getReadCapacityUnits())
                                                                                 .withWriteCapacityUnits(createTableRequest.getProvisionedThroughput().getWriteCapacityUnits()))
                .withStreamSpecification(createTableRequest.getStreamSpecification());
        if (createTableRequest.getLocalSecondaryIndexes() != null) {
                tableDescription.withLocalSecondaryIndexes(createTableRequest.getLocalSecondaryIndexes().stream().map(lsi ->
                    new LocalSecondaryIndexDescription().withIndexName(lsi.getIndexName())
                            .withKeySchema(lsi.getKeySchema())
                            .withProjection(lsi.getProjection())).collect(Collectors.toList()));
        }
        if (createTableRequest.getGlobalSecondaryIndexes() != null) {
            tableDescription.withGlobalSecondaryIndexes(createTableRequest.getGlobalSecondaryIndexes().stream().map(gsi ->
                    new GlobalSecondaryIndexDescription().withIndexName(gsi.getIndexName())
                            .withKeySchema(gsi.getKeySchema())
                            .withProjection(gsi.getProjection())
                            .withProvisionedThroughput(new ProvisionedThroughputDescription().withReadCapacityUnits(gsi.getProvisionedThroughput().getReadCapacityUnits())
                                    .withWriteCapacityUnits(gsi.getProvisionedThroughput().getWriteCapacityUnits()))).collect(Collectors.toList()));
        }
        String tableDataJson = tableDataToJson(tableDescription);
        return new HashMap<>(ImmutableMap.of(
                tableDescriptionTableHashKeyField, new AttributeValue(addPrefix(createTableRequest.getTableName())),
                tableDescriptionTableDataField, new AttributeValue(tableDataJson)));
    }

    private String tableDataToJson(TableDescription tableDescription) {
        return gson.toJson(tableDescription);
    }

    private TableDescription jsonToTableData(String tableDataString) {
        return gson.fromJson(tableDataString, TableDescription.class);
    }

    private String addPrefix(String tableName) {
        return getPrefix() + tableName;
    }

    private String getPrefix() {
        return mtContext.getContext() + delimiter;
    }

    public static class MTDynamoDBTableDescriptionRepoBuilder {
        private AmazonDynamoDB amazonDynamoDB;
        private MTAmazonDynamoDBContextProvider mtContext;
        private String tableDescriptionTableName;
        private String tableDescriptionTableHashKeyField;
        private String tableDescriptionTableDataField;
        private String delimiter;
        private Integer pollIntervalSeconds;
        private Optional<String> tablePrefix = Optional.empty();

        public MTDynamoDBTableDescriptionRepoBuilder withAmazonDynamoDB(AmazonDynamoDB amazonDynamoDB) {
            this.amazonDynamoDB = amazonDynamoDB; return this;
        }

        public MTDynamoDBTableDescriptionRepoBuilder withContext(MTAmazonDynamoDBContextProvider mtContext) {
            this.mtContext = mtContext; return this;
        }

        public MTDynamoDBTableDescriptionRepoBuilder withTableDescriptionTableName(String tableDescriptionTableName) {
            this.tableDescriptionTableName = tableDescriptionTableName; return this;
        }

        @SuppressWarnings("unused")
        public MTDynamoDBTableDescriptionRepoBuilder withTableDescriptionTableHashKeyField(String tableDescriptionTableHashKeyField) {
            this.tableDescriptionTableHashKeyField = tableDescriptionTableHashKeyField; return this;
        }

        @SuppressWarnings("unused")
        public MTDynamoDBTableDescriptionRepoBuilder withTableDescriptionTableDataField(String tableDescriptionTableDataField) {
            this.tableDescriptionTableDataField = tableDescriptionTableDataField; return this;
        }

        @SuppressWarnings("unused")
        public MTDynamoDBTableDescriptionRepoBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter; return this;
        }

        public MTDynamoDBTableDescriptionRepoBuilder withPollIntervalSeconds(int pollIntervalSeconds) {
            this.pollIntervalSeconds = pollIntervalSeconds; return this;
        }

        public MTDynamoDBTableDescriptionRepoBuilder withTablePrefix(Optional<String> tablePrefix) {
            this.tablePrefix = tablePrefix; return this;
        }

        public MTDynamoDBTableDescriptionRepo build() {
            setDefaults();
            validate();
            return new MTDynamoDBTableDescriptionRepo(
                    amazonDynamoDB,
                    mtContext,
                    tableDescriptionTableName,
                    tablePrefix,
                    tableDescriptionTableHashKeyField,
                    tableDescriptionTableDataField,
                    delimiter,
                    pollIntervalSeconds);
        }

        private void validate() {
            checkArgument(amazonDynamoDB != null,"amazonDynamoDB is required");
            checkArgument(mtContext != null,"mtContext is required");
            checkArgument(tableDescriptionTableName != null,"tableDescriptionTableName is required");
        }

        private void setDefaults() {
            if (tableDescriptionTableHashKeyField == null) {
                tableDescriptionTableHashKeyField = TABLEMETADATA_HKFIELD;
            }
            if (tableDescriptionTableDataField == null) {
                tableDescriptionTableDataField = TABLEMETADATA_DATAFIELD;
            }
            if (delimiter == null) {
                delimiter = DELIMITER;
            }
            if (pollIntervalSeconds == null) {
                pollIntervalSeconds = 5;
            }
        }

    }

    private String prefix(String tableName, Optional<String> tablePrefix) {
        return tablePrefix.map(tablePrefix1 -> tablePrefix1 + tableName).orElse(tableName);
    }

}
