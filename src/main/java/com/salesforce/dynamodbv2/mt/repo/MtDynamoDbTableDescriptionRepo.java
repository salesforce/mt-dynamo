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
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.cache.MtCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable;
import com.salesforce.dynamodbv2.mt.util.DynamoDbCapacity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * Stores table definitions in single table.  Each record represents a table.  Table names are prefixed with context.
 *
 * <p>The AmazonDynamoDb that it uses must not, itself, be a MtAmazonDynamoDb* instance.  MtAmazonDynamoDbLogger
 * is supported.
 *
 * @author msgroi
 */
public class MtDynamoDbTableDescriptionRepo implements MtTableDescriptionRepo {

    private static final Gson GSON = new Gson();

    private final AmazonDynamoDB amazonDynamoDb;
    private final AmazonDynamoDbAdminUtils adminUtils;
    private final BillingMode billingMode;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final Optional<String> topLevelContext;
    private final String tableDescriptionTableHashKeyField;
    private final String tableDescriptionTableDataField;
    private final String delimiter;
    private final int pollIntervalSeconds;
    private final MtCache<MtTableDescription> cache;
    private final String prefixedTableDescriptionTableName;
    private final Supplier<TableDescription> tableDescriptionTableSupplier;

    private MtDynamoDbTableDescriptionRepo(AmazonDynamoDB amazonDynamoDb,
                                           BillingMode billingMode,
                                           MtAmazonDynamoDbContextProvider mtContext,
                                           Optional<String> topLevelContext,
                                           String tableDescriptionTableName,
                                           Optional<String> tablePrefix,
                                           String tableDescriptionTableHashKeyField,
                                           String tableDescriptionTableDataField,
                                           String delimiter,
                                           int pollIntervalSeconds,
                                           Cache<Object, MtTableDescription> tableDescriptionCache) {
        this.amazonDynamoDb = amazonDynamoDb;
        this.adminUtils = new AmazonDynamoDbAdminUtils(amazonDynamoDb);
        this.billingMode = billingMode;
        this.mtContext = mtContext;
        this.topLevelContext = topLevelContext;
        this.tableDescriptionTableHashKeyField = tableDescriptionTableHashKeyField;
        this.tableDescriptionTableDataField = tableDescriptionTableDataField;
        this.delimiter = delimiter;
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.cache = new MtCache<>(mtContext, tableDescriptionCache);
        this.prefixedTableDescriptionTableName = prefix(tableDescriptionTableName, tablePrefix);
        this.tableDescriptionTableSupplier = () -> {
            createTableDescriptionTableIfNotExists(pollIntervalSeconds);
            return new TableDescription().withTableName(prefixedTableDescriptionTableName);
        };
    }

    @Override
    public MtTableDescription createTableMetadata(CreateTableRequest createTableRequest, boolean isMultitenant) {
        amazonDynamoDb.putItem(new PutItemRequest().withTableName(getTableDescriptionTableName())
            .withItem(createItem(createTableRequest, isMultitenant)));
        return getTableDescription(createTableRequest.getTableName());
    }

    @Override
    public MtTableDescription getTableDescription(String tableName) {
        return getTableDescriptionFromCache(tableName);
    }

    private MtTableDescription getTableDescriptionFromCache(String tableName) throws ResourceNotFoundException {
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

    private MtTableDescription getTableDescriptionNoCache(String tableName) {
        // first get description for table name at the current context
        Optional<MtTableDescription> tableDescription = getTableDescriptionFromDb(tableName);
        // if no such entry exists at the current context, check if this name corresponds to a multitenant table at the
        // top-level context
        if (tableDescription.isEmpty() && topLevelContext.isPresent()
            && !topLevelContext.get().equals(mtContext.getContext())) {
            tableDescription = mtContext.withContext(topLevelContext.get(), () -> getTableDescriptionFromDb(tableName))
                .filter(MtTableDescription::isMultitenant);
        }
        return tableDescription.orElseThrow(() -> new ResourceNotFoundException(
            "table metadata entry for '" + tableName + "' does not exist in " + getTableDescriptionTableName()));
    }

    private Optional<MtTableDescription> getTableDescriptionFromDb(String tableName) {
        Map<String, AttributeValue> item = amazonDynamoDb.getItem(new GetItemRequest()
            .withTableName(getTableDescriptionTableName())
            .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField,
                new AttributeValue(addPrefix(tableName)))))
            .withConsistentRead(true)).getItem();
        return Optional.ofNullable(item)
            .map(i -> i.get(tableDescriptionTableDataField).getS())
            .map(MtDynamoDbTableDescriptionRepo::jsonToTableData);
    }

    @Override
    public void deleteTableMetadata(String tableName) {
        cache.invalidate(tableName);
        amazonDynamoDb.deleteItem(new DeleteItemRequest()
            .withTableName(getTableDescriptionTableName())
            .withKey(new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField,
                new AttributeValue(addPrefix(tableName))))));
    }

    private TenantTable getTenantTableFromHashKey(String hashKey) {
        String[] parts = hashKey.split(Pattern.quote(delimiter));
        return new TenantTable(parts[1], parts[0]);
    }

    @Override
    public String getTableDescriptionTableName() {
        return tableDescriptionTableSupplier.get().getTableName();
    }

    public void createDefaultDescriptionTable() {
        createTableDescriptionTableIfNotExists(this.pollIntervalSeconds);
    }

    private void createTableDescriptionTableIfNotExists(int pollIntervalSeconds) {
        CreateTableRequest createTableRequest = new CreateTableRequest();
        DynamoDbCapacity.setBillingMode(createTableRequest, this.billingMode);

        adminUtils.createTableIfNotExists(
            createTableRequest.withTableName(prefixedTableDescriptionTableName)
                .withKeySchema(new KeySchemaElement().withAttributeName(tableDescriptionTableHashKeyField)
                    .withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition()
                    .withAttributeName(tableDescriptionTableHashKeyField)
                    .withAttributeType(ScalarAttributeType.S)),
            pollIntervalSeconds);
    }

    private static CreateTableRequest getCreateTableRequest(TableDescription description) {
        return new CreateTableRequest().withTableName(description.getTableName())
            .withKeySchema(description.getKeySchema())
            .withAttributeDefinitions(description.getAttributeDefinitions())
            .withStreamSpecification(description.getStreamSpecification())
            .withProvisionedThroughput(
                getProvisionedThroughput(description.getProvisionedThroughput()))
            .withGlobalSecondaryIndexes(getGlobalIndexes(description.getGlobalSecondaryIndexes()))
            .withLocalSecondaryIndexes(getLocalIndexes(description.getLocalSecondaryIndexes()));
    }

    private static List<GlobalSecondaryIndex> getGlobalIndexes(
        Collection<GlobalSecondaryIndexDescription> descriptions) {
        return descriptions == null ? null : descriptions
            .stream()
            .map(s ->
                new GlobalSecondaryIndex().withIndexName(s.getIndexName())
                    .withKeySchema(s.getKeySchema())
                    .withProjection(s.getProjection())
                    .withProvisionedThroughput(getProvisionedThroughput(s.getProvisionedThroughput())))
            .collect(Collectors.toList());
    }

    private static List<LocalSecondaryIndex> getLocalIndexes(Collection<LocalSecondaryIndexDescription> descriptions) {
        return descriptions == null ? null :
            descriptions.stream()
                .map(s ->
                    new LocalSecondaryIndex()
                        .withIndexName(s.getIndexName())
                        .withKeySchema(s.getKeySchema())
                        .withProjection(s.getProjection()))
                .collect(Collectors.toList());
    }

    private static ProvisionedThroughput getProvisionedThroughput(ProvisionedThroughputDescription description) {
        return description == null ? null :
            new ProvisionedThroughput(description.getReadCapacityUnits(), description.getWriteCapacityUnits());
    }

    private static ProvisionedThroughputDescription getProvisionedThroughputDesc(ProvisionedThroughput throughput) {
        return throughput == null ? null :
            new ProvisionedThroughputDescription()
                .withReadCapacityUnits(throughput.getReadCapacityUnits())
                .withWriteCapacityUnits(throughput.getWriteCapacityUnits());
    }

    private Map<String, AttributeValue> createItem(CreateTableRequest createTableRequest, boolean isMultitenant) {
        MtTableDescription tableDescription = new MtTableDescription();
        tableDescription
            .withTableName(createTableRequest.getTableName())
            .withKeySchema(createTableRequest.getKeySchema())
            .withAttributeDefinitions(createTableRequest.getAttributeDefinitions())
            .withStreamSpecification(createTableRequest.getStreamSpecification())
            .withProvisionedThroughput(getProvisionedThroughputDesc(createTableRequest.getProvisionedThroughput()));

        if (createTableRequest.getLocalSecondaryIndexes() != null) {
            tableDescription.withLocalSecondaryIndexes(createTableRequest.getLocalSecondaryIndexes().stream().map(lsi ->
                new LocalSecondaryIndexDescription().withIndexName(lsi.getIndexName())
                    .withKeySchema(lsi.getKeySchema())
                    .withProjection(lsi.getProjection())).collect(Collectors.toList()));
        }

        if (createTableRequest.getGlobalSecondaryIndexes() != null) {
            tableDescription.withGlobalSecondaryIndexes(createTableRequest.getGlobalSecondaryIndexes().stream().map(
                gsi ->
                    new GlobalSecondaryIndexDescription()
                        .withIndexName(gsi.getIndexName())
                        .withKeySchema(gsi.getKeySchema())
                        .withProjection(gsi.getProjection())
                        .withProvisionedThroughput(getProvisionedThroughputDesc(gsi.getProvisionedThroughput()))
            ).collect(Collectors.toList()));
        }

        // add properties not defined in base TableDescription
        tableDescription.withMultitenant(isMultitenant);

        String tableDataJson = tableDataToJson(tableDescription);
        return new HashMap<>(ImmutableMap.of(
            tableDescriptionTableHashKeyField, new AttributeValue(addPrefix(createTableRequest.getTableName())),
            tableDescriptionTableDataField, new AttributeValue(tableDataJson)));
    }

    private static String tableDataToJson(MtTableDescription tableDescription) {
        return GSON.toJson(tableDescription);
    }

    private static MtTableDescription jsonToTableData(String tableDataString) {
        return GSON.fromJson(tableDataString, MtTableDescription.class);
    }

    private String getHashKey(TenantTable tenantTable) {
        return tenantTable.getTenantName()
            + delimiter
            + tenantTable.getVirtualTableName();
    }

    private String addPrefix(String tableName) {
        return getPrefix() + tableName;
    }

    private String getPrefix() {
        return mtContext.getContext() + delimiter;
    }

    @NotNull
    @Override
    public ListMetadataResult listVirtualTableMetadata(ListMetadataRequest listMetadataRequest) {
        return listVirtualTableMetadata(listMetadataRequest, null);
    }

    /**
     * Do a scan of the description table, returning all virtual table metadata. If a filter is passed, continue
     * scanning until a full page of results is populated, or the description table is exhausted.
     */
    private ListMetadataResult listVirtualTableMetadata(ListMetadataRequest listMetadataRequest, String filterTenant) {
        Preconditions.checkArgument(mtContext.getContextOpt().map(t -> filterTenant.equals(t))
                .orElse(filterTenant == null),
            "Tenant context does not match filter");
        Map<String, AttributeValue> lastEvaluatedKey = null;
        if (listMetadataRequest.getStartTenantTableKey() != null) {
            lastEvaluatedKey = new HashMap<>(ImmutableMap.of(tableDescriptionTableHashKeyField,
                new AttributeValue(getHashKey(listMetadataRequest.getStartTenantTableKey()))));
        }
        ScanRequest scanReq = new ScanRequest(getTableDescriptionTableName());
        // if tenant scoped virtual table list, keep scanning until we either run out of metadata rows in the
        // multitenant virtual table definitions, or we fill up our result set
        List<MtCreateTableRequest> createTableRequests = new ArrayList<>();
        do {
            ScanResult scanResult;
            scanReq.setExclusiveStartKey(lastEvaluatedKey);
            scanResult = amazonDynamoDb.scan(scanReq);
            List<MtCreateTableRequest> createRequestsBatch = scanResult.getItems().stream()
                .map(this::fromRowToMtCreateTableRequest)
                .collect(Collectors.toList());
            if (filterTenant == null) {
                createTableRequests.addAll(createRequestsBatch);
            } else {
                createTableRequests.addAll(createRequestsBatch.stream()
                    .filter(t -> t.getTenantName().equals(filterTenant))
                    .collect(Collectors.toList()));
            }
            lastEvaluatedKey = scanResult.getLastEvaluatedKey();
        } while (lastEvaluatedKey != null && createTableRequests.size() < scanReq.getLimit());

        if (createTableRequests.size() >= listMetadataRequest.getLimit()) {
            List<MtCreateTableRequest> ret = createTableRequests.subList(0, listMetadataRequest.getLimit());
            return new ListMetadataResult(ret, Iterables.getLast(ret));
        } else {
            return new ListMetadataResult(createTableRequests, null);
        }

    }

    private MtCreateTableRequest fromRowToMtCreateTableRequest(Map<String, AttributeValue> item) {
        MtTableDescription tableDescription = jsonToTableData(item.get(tableDescriptionTableDataField).getS());
        TenantTable tenantTable = getTenantTableFromHashKey(item.get(tableDescriptionTableHashKeyField).getS());
        // TODO include properties defined in MtTableDescription but not in TableDescription
        return new MtCreateTableRequest(tenantTable.getTenantName(), getCreateTableRequest(tableDescription));
    }

    @NotNull
    @Override
    public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
        Preconditions.checkArgument(mtContext.getContextOpt().isPresent(),
            "Must provide a tenant context.");
        ListMetadataRequest listMetadataRequest = new ListMetadataRequest()
            .withLimit(listTablesRequest.getLimit())
            .withExclusiveStartKey(listTablesRequest.getExclusiveStartTableName() == null
                ? null
                : new TenantTable(listTablesRequest.getExclusiveStartTableName(),
                mtContext.getContext()));
        ListMetadataResult listMetadataResult = listVirtualTableMetadata(listMetadataRequest, mtContext.getContext());
        return new ListTablesResult()
            .withLastEvaluatedTableName(
                Optional.ofNullable(listMetadataResult.getLastEvaluatedTable())
                    .map(t -> t.getCreateTableRequest().getTableName())
                    .orElse(null))
            .withTableNames(listMetadataResult.getCreateTableRequests()
                .stream().map(t -> t.getCreateTableRequest().getTableName())
                .collect(Collectors.toList()));

    }

    public static MtDynamoDbTableDescriptionRepoBuilder builder() {
        return new MtDynamoDbTableDescriptionRepoBuilder();
    }

    public static class MtDynamoDbTableDescriptionRepoBuilder {

        private static final String DEFAULT_TABLE_METADATA_HK_FIELD = "table";
        private static final String DEFAULT_TABLE_METADATA_DATA_FIELD = "data";
        private static final String DEFAULT_DELIMITER = ".";

        private AmazonDynamoDB amazonDynamoDb;
        private MtAmazonDynamoDbContextProvider mtContext;
        private Optional<String> topLevelContext = Optional.empty();
        private String tableDescriptionTableName;
        private String tableDescriptionTableHashKeyField;
        private String tableDescriptionTableDataField;
        private String delimiter;
        private Integer pollIntervalSeconds;
        private BillingMode billingMode;
        private Optional<String> tablePrefix = Optional.empty();
        private Cache<Object, MtTableDescription> tableDescriptionCache;

        public MtDynamoDbTableDescriptionRepoBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTopLevelContext(Optional<String> topLevelContext) {
            this.topLevelContext = topLevelContext;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withTableDescriptionTableName(String tableDescriptionTableName) {
            this.tableDescriptionTableName = tableDescriptionTableName;
            return this;
        }

        public MtDynamoDbTableDescriptionRepoBuilder withBillingMode(BillingMode billingMode) {
            this.billingMode = billingMode;
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

        public MtDynamoDbTableDescriptionRepoBuilder withTableDescriptionCache(
            Cache<Object, MtTableDescription> tableDescriptionCache) {
            this.tableDescriptionCache = tableDescriptionCache;
            return this;
        }

        /**
         * Builder. Build!
         *
         * @return a newly created {@code MtDynamoDbTableDescriptionRepo} based on the contents of the
         * {@code MtDynamoDbTableDescriptionRepoBuilder}
         */
        public MtDynamoDbTableDescriptionRepo build() {
            setDefaults();
            validate();
            return new MtDynamoDbTableDescriptionRepo(
                amazonDynamoDb,
                billingMode,
                mtContext,
                topLevelContext,
                tableDescriptionTableName,
                tablePrefix,
                tableDescriptionTableHashKeyField,
                tableDescriptionTableDataField,
                delimiter,
                pollIntervalSeconds,
                tableDescriptionCache);
        }

        private void validate() {
            checkArgument(amazonDynamoDb != null, "amazonDynamoDb is required");
            checkArgument(mtContext != null, "mtContext is required");
            checkArgument(tableDescriptionTableName != null, "tableDescriptionTableName is required");
        }

        private void setDefaults() {
            if (tableDescriptionTableHashKeyField == null) {
                tableDescriptionTableHashKeyField = DEFAULT_TABLE_METADATA_HK_FIELD;
            }
            if (tableDescriptionTableDataField == null) {
                tableDescriptionTableDataField = DEFAULT_TABLE_METADATA_DATA_FIELD;
            }
            if (delimiter == null) {
                delimiter = DEFAULT_DELIMITER;
            }
            if (pollIntervalSeconds == null) {
                pollIntervalSeconds = 5;
            }
            if (tableDescriptionCache == null) {
                tableDescriptionCache = CacheBuilder.newBuilder().build();
            }
        }

    }

    private String prefix(String tableName, Optional<String> tablePrefix) {
        return tablePrefix.map(tablePrefix1 -> tablePrefix1 + tableName).orElse(tableName);
    }

}
