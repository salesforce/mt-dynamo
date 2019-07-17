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
import com.amazonaws.services.dynamodbv2.model.BillingMode;
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
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.util.DynamoDbCapacity;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Allows for dividing tenants into their own tables by prefixing table names
 * with the multitenant context.
 *
 * <p>The multitenant context is separated from the table name by the delimiter,
 * which is '.' by default.
 *
 * <p>To use, call the static builder() method. The following parameters are
 * required ... - an AmazonDynamoDB instance - a multitenant context
 *
 * <p>The following are optional arguments ... - delimiter: a String delimiter used
 * to separate the tenant identifier prefix from the table name
 *
 * <p>Supported: batchGet|get|put Item, create|describe|delete Table, scan, query
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbByTable extends MtAmazonDynamoDbBase {

    private final String delimiter;
    private final Optional<String> tablePrefix;
    private final BillingMode billingMode;
    private final String scanTenantKey;
    private final String scanVirtualTableKey;

    private MtAmazonDynamoDbByTable(MtAmazonDynamoDbContextProvider mtContext, AmazonDynamoDB amazonDynamoDb,
                                    MeterRegistry meterRegistry, BillingMode billingMode, String delimiter,
                                    Optional<String> tablePrefix, String scanTenantKey, String scanVirtualTableKey) {
        super(mtContext, amazonDynamoDb, meterRegistry);
        this.billingMode = billingMode;
        // TODO add billingMode support
        this.delimiter = delimiter;
        this.tablePrefix = tablePrefix;
        this.scanTenantKey = scanTenantKey;
        this.scanVirtualTableKey = scanVirtualTableKey;
    }

    @Override
    protected boolean isMtTable(String tableName) {
        String prefix = tablePrefix.orElse("");
        return tableName.startsWith(prefix) && tableName.indexOf(delimiter, prefix.length()) >= 0;
    }

    /**
     * Transform unqualified table names in request to qualified (by tenant) table names, make the dynamo request, then
     * transform qualified table names back into unqualified table names in the response.
     */
    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        final BatchGetItemRequest batchGetItemRequestWithPrefixedTableNames = batchGetItemRequest.clone();
        batchGetItemRequestWithPrefixedTableNames.clearRequestItemsEntries();
        for (String unqualifiedTableName : batchGetItemRequest.getRequestItems().keySet()) {
            batchGetItemRequestWithPrefixedTableNames.addRequestItemsEntry(buildPrefixedTableName(unqualifiedTableName),
                batchGetItemRequest.getRequestItems().get(unqualifiedTableName));
        }

        final BatchGetItemResult batchGetItemResult = getAmazonDynamoDb()
            .batchGetItem(batchGetItemRequestWithPrefixedTableNames);

        final Map<String, List<Map<String, AttributeValue>>> responsesWithUnprefixedTableNames
            = new HashMap<>();
        for (String qualifiedTableName : batchGetItemResult.getResponses().keySet()) {
            responsesWithUnprefixedTableNames.put(stripTableNamePrefix(qualifiedTableName),
                batchGetItemResult.getResponses().get(qualifiedTableName));
        }
        batchGetItemResult.clearResponsesEntries();
        batchGetItemResult.setUnprocessedKeys(batchGetItemResult.getUnprocessedKeys().entrySet().stream().collect(
            Collectors.toMap(entry -> stripTableNamePrefix(entry.getKey()), Entry::getValue)));
        responsesWithUnprefixedTableNames.forEach(batchGetItemResult::addResponsesEntry);
        return batchGetItemResult;
    }

    /**
     * Create a physical table within dynamo with the given @param createTableRequest table name
     * prefixed by the tenant context.
     */
    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        DynamoDbCapacity.setBillingMode(createTableRequest, billingMode);
        CreateTableRequest request = createTableRequest.clone()
            .withTableName(buildPrefixedTableName(createTableRequest.getTableName()));
        CreateTableResult result = getAmazonDynamoDb().createTable(request);
        TableDescription description = result.getTableDescription();
        description.setTableName(createTableRequest.getTableName());
        setTenantStreamArn(description);
        return result;
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        deleteItemRequest = deleteItemRequest.clone();
        deleteItemRequest.withTableName(buildPrefixedTableName(deleteItemRequest.getTableName()));
        return getAmazonDynamoDb().deleteItem(deleteItemRequest);
    }

    /**
     * Delete the physical dynamo table with the deleteTableRequest's table name prefixed by the current tenant
     * context.
     */
    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        String unqualifiedTableName = deleteTableRequest.getTableName();
        deleteTableRequest = deleteTableRequest.clone();
        deleteTableRequest.withTableName(buildPrefixedTableName(deleteTableRequest.getTableName()));
        DeleteTableResult deleteTableResult = getAmazonDynamoDb().deleteTable(deleteTableRequest);
        deleteTableResult.getTableDescription().setTableName(unqualifiedTableName);
        return deleteTableResult;
    }

    /**
     * Describe the table with describeTableRequest's table name prefixed by the current tenant context.
     */
    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        String unqualifiedTableName = describeTableRequest.getTableName();
        describeTableRequest = describeTableRequest.clone();
        describeTableRequest.withTableName(buildPrefixedTableName(describeTableRequest.getTableName()));
        DescribeTableResult describeTableResult = getAmazonDynamoDb().describeTable(describeTableRequest);
        TableDescription description = describeTableResult.getTable();
        description.setTableName(unqualifiedTableName);
        setTenantStreamArn(description);
        return describeTableResult;
    }

    private void setTenantStreamArn(TableDescription description) {
        if (Optional.ofNullable(description.getStreamSpecification()).map(StreamSpecification::isStreamEnabled)
            .orElse(false)) {
            description.setLatestStreamArn(StreamArn
                .fromString(description.getLatestStreamArn(), getMtContext().getContext(), description.getTableName())
                .toString());
        }
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        getItemRequest = getItemRequest.clone();
        String prefixedTableName = buildPrefixedTableName(getItemRequest.getTableName());
        getItemRequest.withTableName(prefixedTableName);
        return getAmazonDynamoDb().getItem(getItemRequest);
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        putItemRequest = putItemRequest.clone();
        putItemRequest.withTableName(buildPrefixedTableName(putItemRequest.getTableName()));
        return getAmazonDynamoDb().putItem(putItemRequest);
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        queryRequest = queryRequest.clone();
        queryRequest.withTableName(buildPrefixedTableName(queryRequest.getTableName()));
        return getAmazonDynamoDb().query(queryRequest);
    }

    /**
     * Run a scan on the given table specified scanRequest's table name prefixed by the current tenant context.
     * If the context is not specified, run a scan against the @param scanRequest raw table name, with tenant context
     * info encoded into the result item map keyed by {@link scanTenantKey} and {@link scanVirtualTableKey}.
     */
    @Override
    public ScanResult scan(ScanRequest scanRequest) {
        if (getMtContext().getContextOpt().isEmpty()) {
            Preconditions.checkArgument(isMtTable(scanRequest.getTableName()));
            String[] tenantTable = getTenantAndTableName(scanRequest.getTableName());
            ScanResult result = getAmazonDynamoDb().scan(scanRequest);
            result.getItems().forEach(row -> {
                row.put(scanTenantKey, new AttributeValue(tenantTable[1]));
                row.put(scanVirtualTableKey, new AttributeValue(tenantTable[0]));
            });
            return result;
        } else {
            scanRequest = scanRequest.clone();
            scanRequest.withTableName(buildPrefixedTableName(scanRequest.getTableName()));
            return getAmazonDynamoDb().scan(scanRequest);
        }
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        updateItemRequest = updateItemRequest.clone();
        updateItemRequest.withTableName(buildPrefixedTableName(updateItemRequest.getTableName()));
        return getAmazonDynamoDb().updateItem(updateItemRequest);
    }

    public static MtAmazonDynamoDbBuilder builder() {
        return new MtAmazonDynamoDbBuilder();
    }

    public static class MtAmazonDynamoDbBuilder implements TableBuilder {

        private AmazonDynamoDB amazonDynamoDb;
        private MtAmazonDynamoDbContextProvider mtContext;
        private MeterRegistry meterRegistry;
        private BillingMode billingMode;
        private String delimiter;
        private String scanTenantKey = DEFAULT_SCAN_TENANT_KEY;
        private String scanVirtualTableKey = DEFAULT_SCAN_VIRTUAL_TABLE_KEY;
        private Optional<String> tablePrefix = Optional.empty();

        public MtAmazonDynamoDbBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
            return this;
        }

        @Override
        public MtAmazonDynamoDbBuilder withBillingMode(BillingMode billingMode) {
            this.billingMode = billingMode;
            return this;
        }

        @Override
        public TableBuilder withScanTenantKey(String scanTenantKey) {
            this.scanTenantKey = scanTenantKey;
            return this;
        }

        @Override
        public TableBuilder withScanVirtualTableKey(String scanVirtualTableKey) {
            this.scanVirtualTableKey = scanVirtualTableKey;
            return this;
        }

        public MtAmazonDynamoDbBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        MtAmazonDynamoDbBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public MtAmazonDynamoDbBuilder withTablePrefix(String tablePrefix) {
            this.tablePrefix = Optional.of(tablePrefix);
            return this;
        }

        public MtAmazonDynamoDbBuilder withMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        /**
         * Useful comment.
         *
         * @return a newly created {@code MtAmazonDynamoDbByTable} based on the contents of the
         * {@code MtAmazonDynamoDbBuilder}
         */
        public MtAmazonDynamoDbByTable build() {
            setDefaults();
            Preconditions.checkNotNull(amazonDynamoDb, "amazonDynamoDb is required");
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            return new MtAmazonDynamoDbByTable(mtContext, amazonDynamoDb, meterRegistry, billingMode, delimiter,
                tablePrefix, scanTenantKey, scanVirtualTableKey);
        }

        private void setDefaults() {
            if (delimiter == null) {
                delimiter = ".";
            }
            if (billingMode == null) {
                billingMode = BillingMode.PROVISIONED;
            }
            if (meterRegistry == null) {
                meterRegistry = new CompositeMeterRegistry();
            }
        }

    }

    private String getTableNamePrefix() {
        return this.tablePrefix.orElse("") + getMtContext().getContext() + this.delimiter;
    }

    @VisibleForTesting
    private String buildPrefixedTableName(String unqualifiedTableName) {
        return getTableNamePrefix() + unqualifiedTableName;
    }

    @VisibleForTesting
    private String stripTableNamePrefix(String qualifiedTableName) {
        final String tableNamePrefix = getTableNamePrefix();
        Preconditions.checkState(qualifiedTableName.startsWith(tableNamePrefix));
        return qualifiedTableName.substring(tableNamePrefix.length());
    }

    String[] getTenantAndTableName(String qualifiedTableName) {
        int start = this.tablePrefix.orElse("").length();
        int idx = qualifiedTableName.indexOf(this.delimiter, start);
        Preconditions.checkArgument(idx > 0);
        String tenant = qualifiedTableName.substring(start, idx);
        String name = qualifiedTableName.substring(idx + delimiter.length());
        return new String[] { tenant, name };
    }

}
