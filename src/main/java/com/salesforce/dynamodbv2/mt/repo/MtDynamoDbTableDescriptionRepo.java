/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.SharedTableNamingRules.VIRTUAL_MULTITENANT_TABLE_PREFIX;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.annotations.VisibleForTesting;
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
    private final Optional<String> globalContext;
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
                                           Optional<String> globalContext,
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
        this.globalContext = globalContext;
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
        // first get description for table name in the current context
        Optional<MtTableDescription> tableDescription = getTableDescriptionFromDb(tableName);
        // if no entry exists, check if this name corresponds to a multitenant table in empty context
        if (tableDescription.isEmpty() && globalContext.isPresent()
            && !globalContext.get().equals(mtContext.getContext())) {
            tableDescription = mtContext.withContext(globalContext.get(), () -> getTableDescriptionFromDb(tableName))
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

    private static ProvisionedThroughputDescription toProvisionedThroughputDesc(ProvisionedThroughput throughput) {
        return throughput == null ? null :
            new ProvisionedThroughputDescription()
                .withReadCapacityUnits(throughput.getReadCapacityUnits())
                .withWriteCapacityUnits(throughput.getWriteCapacityUnits());
    }

    @VisibleForTesting
    static MtTableDescription toTableDescription(CreateTableRequest createTableRequest, boolean isMultitenant) {
        MtTableDescription tableDescription = new MtTableDescription();
        tableDescription
            .withTableName(createTableRequest.getTableName())
            .withKeySchema(createTableRequest.getKeySchema())
            .withAttributeDefinitions(createTableRequest.getAttributeDefinitions())
            .withStreamSpecification(createTableRequest.getStreamSpecification())
            .withProvisionedThroughput(toProvisionedThroughputDesc(createTableRequest.getProvisionedThroughput()));

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
                        .withProvisionedThroughput(toProvisionedThroughputDesc(gsi.getProvisionedThroughput()))
            ).collect(Collectors.toList()));
        }

        // add properties not defined in base TableDescription
        tableDescription.withMultitenant(isMultitenant);

        return tableDescription;
    }

    private Map<String, AttributeValue> createItem(CreateTableRequest createTableRequest, boolean isMultitenant) {
        String tableDataJson = tableDataToJson(toTableDescription(createTableRequest, isMultitenant));
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

    private String addPrefix(String tableName) {
        return addPrefix(mtContext.getContext(), tableName);
    }

    private String addPrefix(String context, String tableName) {
        return context + delimiter + tableName;
    }

    private String toStartHashKey(TenantTable tenantTable) {
        if (tenantTable == null) {
            return null;
        }
        return addPrefix(tenantTable.getTenantName(), tenantTable.getVirtualTableName());
    }

    private String toStartHashKey(String tableName) {
        if (tableName == null) {
            return null;
        }
        String context = tableName.startsWith(VIRTUAL_MULTITENANT_TABLE_PREFIX)
            ? globalContext.get()
            : mtContext.getContext();
        return addPrefix(context, tableName);
    }

    @NotNull
    @Override
    public ListMetadataResult listVirtualTableMetadata(ListMetadataRequest listMetadataRequest) {
        String startHashKey = toStartHashKey(listMetadataRequest.getStartTenantTableKey());
        // TODO support backing up
        return listVirtualTableMetadata(startHashKey, listMetadataRequest.getLimit(), null);
    }

    /**
     * Do a scan of the description table, returning all virtual table metadata. If a filter is passed, continue
     * scanning until a full page of results is populated, or the description table is exhausted.
     */
    private ListMetadataResult listVirtualTableMetadata(String startHashKey, Integer limit, String filterTenant) {
        Preconditions.checkArgument(mtContext.getContextOpt().map(t -> filterTenant.equals(t))
                .orElse(filterTenant == null),
            "Tenant context does not match filter");
        Map<String, AttributeValue> lastEvaluatedKey = startHashKey == null
            ? null
            : ImmutableMap.of(tableDescriptionTableHashKeyField, new AttributeValue(startHashKey));
        ScanRequest scanReq = new ScanRequest(getTableDescriptionTableName());

        // if tenant scoped virtual table list, keep scanning until we either run out of metadata rows in the
        // virtual table definitions, or we fill up our result set
        List<MtTenantTableDesciption> tableDescriptions = new ArrayList<>();
        do {
            ScanResult scanResult;
            scanReq.setExclusiveStartKey(lastEvaluatedKey);
            scanResult = amazonDynamoDb.scan(scanReq);
            List<MtTenantTableDesciption> tableDescriptionsBatch = scanResult.getItems().stream()
                .map(this::toMtTenantTableDescription)
                .collect(Collectors.toList());
            if (filterTenant == null) {
                tableDescriptions.addAll(tableDescriptionsBatch);
            } else {
                // include all descriptions with a matching tenant name, plus any multitenant tables
                tableDescriptions.addAll(tableDescriptionsBatch.stream()
                    .filter(t -> t.getTenantName().equals(filterTenant) || t.getTableDescription().isMultitenant())
                    .collect(Collectors.toList()));
            }
            lastEvaluatedKey = scanResult.getLastEvaluatedKey();
        } while (lastEvaluatedKey != null && tableDescriptions.size() < scanReq.getLimit());

        if (limit != null && tableDescriptions.size() >= limit) {
            List<MtTenantTableDesciption> ret = tableDescriptions.subList(0, limit);
            return new ListMetadataResult(ret, Iterables.getLast(ret));
        } else {
            return new ListMetadataResult(tableDescriptions, null);
        }
    }

    private MtTenantTableDesciption toMtTenantTableDescription(Map<String, AttributeValue> item) {
        MtTableDescription tableDescription = jsonToTableData(item.get(tableDescriptionTableDataField).getS());
        TenantTable tenantTable = getTenantTableFromHashKey(item.get(tableDescriptionTableHashKeyField).getS());
        return new MtTenantTableDesciption(tenantTable.getTenantName(), tableDescription);
    }

    @NotNull
    @Override
    public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
        Preconditions.checkArgument(mtContext.getContextOpt().isPresent(), "Must provide a tenant context.");
        String startHashKey = toStartHashKey(listTablesRequest.getExclusiveStartTableName());
        ListMetadataResult listMetadataResult = listVirtualTableMetadata(startHashKey, listTablesRequest.getLimit(),
            mtContext.getContext());
        return new ListTablesResult()
            .withLastEvaluatedTableName(
                Optional.ofNullable(listMetadataResult.getLastEvaluatedTable())
                    .map(t -> t.getTableDescription().getTableName())
                    .orElse(null))
            .withTableNames(listMetadataResult.getTenantTableDescriptions()
                .stream().map(t -> t.getTableDescription().getTableName())
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
        private Optional<String> globalContext;
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

        public MtDynamoDbTableDescriptionRepoBuilder withGlobalContext(Optional<String> globalContext) {
            this.globalContext = globalContext;
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
                globalContext,
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
