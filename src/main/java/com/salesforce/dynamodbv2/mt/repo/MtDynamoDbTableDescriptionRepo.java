/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import static com.google.common.base.Preconditions.checkArgument;

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
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.cache.MtCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Stores table definitions in single table.  Each record represents a table.  Table names are prefixed with context.
 *
 * <p>The AmazonDynamoDb that it uses must not, itself, be a MtAmazonDynamoDb* instance.  MtAmazonDynamoDbLogger
 * is supported.
 *
 * @author msgroi
 */
public class MtDynamoDbTableDescriptionRepo implements MtTableDescriptionRepo {

    private static final String TABLEMETADATA_HKFIELD = "table";
    private static final String TABLEMETADATA_DATAFIELD = "data";
    private static final String DELIMITER = ".";

    private static final Gson GSON = new Gson();
    private final AmazonDynamoDB amazonDynamoDb;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final AmazonDynamoDbAdminUtils adminUtils;
    private final String tableDescriptionTableName;
    private final String tableDescriptionTableHashKeyField;
    private final String tableDescriptionTableDataField;
    private final String delimiter;
    private final int pollIntervalSeconds;
    private final MtCache<TableDescription> cache;

    private MtDynamoDbTableDescriptionRepo(AmazonDynamoDB amazonDynamoDb,
                                           MtAmazonDynamoDbContextProvider mtContext,
                                           String tableDescriptionTableName,
                                           Optional<String> tablePrefix,
                                           String tableDescriptionTableHashKeyField,
                                           String tableDescriptionTableDataField,
                                           String delimiter,
                                           int pollIntervalSeconds) {
        this.amazonDynamoDb = amazonDynamoDb;
        this.mtContext = mtContext;
        adminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.tableDescriptionTableName = prefix(tableDescriptionTableName, tablePrefix);
        this.tableDescriptionTableHashKeyField = tableDescriptionTableHashKeyField;
        this.tableDescriptionTableDataField = tableDescriptionTableDataField;
        this.delimiter = delimiter;
        this.pollIntervalSeconds = pollIntervalSeconds;
        cache = new MtCache<>(mtContext);
    }

    @Override
    public TableDescription createTable(CreateTableRequest createTableRequest) {
        amazonDynamoDb.putItem(new PutItemRequest().withTableName(getTableDescriptionTableName())
            .withItem(createItem(createTableRequest)));
        return getTableDescription(createTableRequest.getTableName());
    }

    @Override
    public TableDescription getTableDescription(String tableName) {
        return getTableDescriptionFromCache(tableName);
    }

    public static MtDynamoDbTableDescriptionRepoBuilder builder() {
        return new MtDynamoDbTableDescriptionRepoBuilder();
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
        Map<String, AttributeValue> item = amazonDynamoDb.getItem(new GetItemRequest()
            .withTableName(getTableDescriptionTableName())
            .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField,
                new AttributeValue(addPrefix(tableName)))))).getItem();
        if (item == null) {
            throw new ResourceNotFoundException("table metadata entry for '" + tableName + "' does not exist in "
                + tableDescriptionTableName);
        }
        String tableDataJson = item.get(tableDescriptionTableDataField).getS();
        return jsonToTableData(tableDataJson);
    }

    @Override
    public TableDescription deleteTable(String tableName) {
        TableDescription tableDescription = getTableDescription(tableName);

        cache.invalidate(tableName);

        amazonDynamoDb.deleteItem(new DeleteItemRequest()
            .withTableName(getTableDescriptionTableName())
            .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField,
                new AttributeValue(addPrefix(tableName))))));

        return tableDescription;
    }

    @Override
    public void invalidateCaches() {
        cache.invalidateAll();
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
                .withAttributeDefinitions(new AttributeDefinition()
                    .withAttributeName(tableDescriptionTableHashKeyField)
                    .withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            pollIntervalSeconds);
    }

    private Map<String, AttributeValue> createItem(CreateTableRequest createTableRequest) {
        TableDescription tableDescription = new TableDescription()
            .withTableName(createTableRequest.getTableName())
            .withKeySchema(createTableRequest.getKeySchema())
            .withAttributeDefinitions(createTableRequest.getAttributeDefinitions())
            .withProvisionedThroughput(new ProvisionedThroughputDescription()
                .withReadCapacityUnits(createTableRequest.getProvisionedThroughput().getReadCapacityUnits())
                .withWriteCapacityUnits(createTableRequest.getProvisionedThroughput().getWriteCapacityUnits()))
            .withStreamSpecification(createTableRequest.getStreamSpecification());
        if (createTableRequest.getLocalSecondaryIndexes() != null) {
            tableDescription.withLocalSecondaryIndexes(createTableRequest.getLocalSecondaryIndexes().stream().map(lsi ->
                new LocalSecondaryIndexDescription().withIndexName(lsi.getIndexName())
                    .withKeySchema(lsi.getKeySchema())
                    .withProjection(lsi.getProjection())).collect(Collectors.toList()));
        }
        if (createTableRequest.getGlobalSecondaryIndexes() != null) {
            tableDescription.withGlobalSecondaryIndexes(createTableRequest.getGlobalSecondaryIndexes().stream().map(
                gsi -> new GlobalSecondaryIndexDescription().withIndexName(gsi.getIndexName())
                    .withKeySchema(gsi.getKeySchema())
                    .withProjection(gsi.getProjection())
                    .withProvisionedThroughput(new ProvisionedThroughputDescription()
                        .withReadCapacityUnits(gsi.getProvisionedThroughput().getReadCapacityUnits())
                        .withWriteCapacityUnits(gsi.getProvisionedThroughput().getWriteCapacityUnits())))
                .collect(Collectors.toList()));
        }
        String tableDataJson = tableDataToJson(tableDescription);
        return new HashMap<>(ImmutableMap.of(
            tableDescriptionTableHashKeyField, new AttributeValue(addPrefix(createTableRequest.getTableName())),
            tableDescriptionTableDataField, new AttributeValue(tableDataJson)));
    }

    private String tableDataToJson(TableDescription tableDescription) {
        return GSON.toJson(tableDescription);
    }

    private TableDescription jsonToTableData(String tableDataString) {
        return GSON.fromJson(tableDataString, TableDescription.class);
    }

    private String addPrefix(String tableName) {
        return getPrefix() + tableName;
    }

    private String getPrefix() {
        return mtContext.getContext() + delimiter;
    }

    public static class MtDynamoDbTableDescriptionRepoBuilder {
        private AmazonDynamoDB amazonDynamoDb;
        private MtAmazonDynamoDbContextProvider mtContext;
        private String tableDescriptionTableName;
        private String tableDescriptionTableHashKeyField;
        private String tableDescriptionTableDataField;
        private String delimiter;
        private Integer pollIntervalSeconds;
        private Optional<String> tablePrefix = Optional.empty();

        public MtDynamoDbTableDescriptionRepoBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTableDescriptionTableName(String tableDescriptionTableName) {
            this.tableDescriptionTableName = tableDescriptionTableName;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTableDescriptionTableHashKeyField(
            String tableDescriptionTableHashKeyField) {
            this.tableDescriptionTableHashKeyField = tableDescriptionTableHashKeyField;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTableDescriptionTableDataField(
            String tableDescriptionTableDataField) {
            this.tableDescriptionTableDataField = tableDescriptionTableDataField;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withPollIntervalSeconds(int pollIntervalSeconds) {
            this.pollIntervalSeconds = pollIntervalSeconds;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTablePrefix(Optional<String> tablePrefix) {
            this.tablePrefix = tablePrefix;
            return this;
        }

        /**
         * TODO: write Javadoc.
         *
         * @return a newly created {@code MtDynamoDbTableDescriptionRepo} based on the contents of the
         *     {@code MtDynamoDbTableDescriptionRepoBuilder}
         */
        public MtDynamoDbTableDescriptionRepo build() {
            setDefaults();
            validate();
            return new MtDynamoDbTableDescriptionRepo(
                amazonDynamoDb,
                mtContext,
                tableDescriptionTableName,
                tablePrefix,
                tableDescriptionTableHashKeyField,
                tableDescriptionTableDataField,
                delimiter,
                pollIntervalSeconds);
        }

        private void validate() {
            checkArgument(amazonDynamoDb != null, "amazonDynamoDb is required");
            checkArgument(mtContext != null, "mtContext is required");
            checkArgument(tableDescriptionTableName != null, "tableDescriptionTableName is required");
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
