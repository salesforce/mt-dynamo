/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BackupDescription;
import com.amazonaws.services.dynamodbv2.model.BackupDetails;
import com.amazonaws.services.dynamodbv2.model.BackupNotFoundException;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ContinuousBackupsUnavailableException;
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest;
import com.amazonaws.services.dynamodbv2.model.CreateBackupResult;
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
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.backups.MtBackupAwsAdaptorKt;
import com.salesforce.dynamodbv2.mt.backups.MtBackupException;
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager;
import com.salesforce.dynamodbv2.mt.backups.MtBackupMetadata;
import com.salesforce.dynamodbv2.mt.backups.RestoreMtBackupRequest;
import com.salesforce.dynamodbv2.mt.backups.SnapshotRequest;
import com.salesforce.dynamodbv2.mt.backups.SnapshotResult;
import com.salesforce.dynamodbv2.mt.backups.TenantBackupMetadata;
import com.salesforce.dynamodbv2.mt.backups.TenantRestoreMetadata;
import com.salesforce.dynamodbv2.mt.backups.TenantTableBackupMetadata;
import com.salesforce.dynamodbv2.mt.cache.MtCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RequestWrapper.AbstractRequestWrapper;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps virtual tables to a set of 7 physical tables hard-coded into the builder by comparing the types of the elements
 * of the virtual table's primary key against the corresponding types on the physical tables.  It support mapping many
 * virtual tables to a single physical table, mapping field names and types, secondary indexes.  It supports for
 * allowing multitenant context to be added to table and index hash key fields.
 *
 * <p>See {@code SharedTableBuilder} for further details.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbBySharedTable extends MtAmazonDynamoDbBase {

    private static final Logger log = LoggerFactory.getLogger(MtAmazonDynamoDbBySharedTable.class);

    private final String name;

    private final MtTableDescriptionRepo mtTableDescriptionRepo;
    private final Cache<Object, Optional<TableMapping>> tableMappingCache;
    private final TableMappingFactory tableMappingFactory;
    private final TablePartitioningStrategy partitioningStrategy;
    private final PhysicalTableManager physicalTableManager;
    private final boolean deleteTableAsync;
    private final boolean truncateOnDeleteTable;
    private final long getRecordsTimeLimit;
    private final Clock clock;
    private final Optional<MtBackupManager> backupManager;
    private final String backupTablePrefix;

    private final SharedTableMappingDelegate delegate;

    /**
     * Shared-table constructor.
     *
     * @param name                   the name of the multitenant AmazonDynamoDB instance
     * @param mtContext              the multitenant context provider
     * @param amazonDynamoDb         the underlying {@code AmazonDynamoDB} delegate
     * @param tableMappingFactory    the table-mapping factory for mapping virtual to physical table instances
     * @param partitioningStrategy   the table partitioning strategy
     * @param mtTableDescriptionRepo the {@code MtTableDescriptionRepo} impl
     * @param physicalTableManager   manager that creates, deletes, and describes physical tables
     * @param deleteTableAsync       flag indicating whether to perform delete-table operations async (vs. sync)
     * @param truncateOnDeleteTable  flag indicating whether to delete all table data when a virtual table is deleted
     * @param getRecordsTimeLimit    soft time limit for getting records out of the shared stream.
     * @param clock                  clock instance to use for enforcing time limit (injected for unit tests).
     * @param tableMappingCache      Guava cache instance used to start virtual-table-to-physical-table description
     * @param meterRegistry          MeterRegistry for reporting metrics.
     * @param scanTenantKey          name of column in multitenant scans to return tenant key encoded into scan result
     *                               set
     * @param scanVirtualTableKey    name of column in multitenant scans to return virtual table name encoded into
     *                               result
     */
    public MtAmazonDynamoDbBySharedTable(String name,
                                         MtAmazonDynamoDbContextProvider mtContext,
                                         AmazonDynamoDB amazonDynamoDb,
                                         TableMappingFactory tableMappingFactory,
                                         TablePartitioningStrategy partitioningStrategy,
                                         MtTableDescriptionRepo mtTableDescriptionRepo,
                                         PhysicalTableManager physicalTableManager,
                                         boolean deleteTableAsync,
                                         boolean truncateOnDeleteTable,
                                         long getRecordsTimeLimit,
                                         Clock clock,
                                         Cache<Object, Optional<TableMapping>> tableMappingCache,
                                         MeterRegistry meterRegistry,
                                         Optional<MtSharedTableBackupManagerBuilder> backupManager,
                                         String scanTenantKey,
                                         String scanVirtualTableKey,
                                         String backupTablePrefix) {
        super(mtContext, amazonDynamoDb, meterRegistry, scanTenantKey, scanVirtualTableKey);
        this.name = name;
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        this.tableMappingCache = new MtCache<>(mtContext, tableMappingCache);
        this.tableMappingFactory = tableMappingFactory;
        this.partitioningStrategy = partitioningStrategy;
        this.physicalTableManager = physicalTableManager;
        this.deleteTableAsync = deleteTableAsync;
        this.truncateOnDeleteTable = truncateOnDeleteTable;
        this.getRecordsTimeLimit = getRecordsTimeLimit;
        this.clock = clock;

        // a reference to this object is leaked to the backup manager in order to run multitenant scans, is this
        // the best way? We need a reference to the backup manager here to serve createBackup and other backup API
        // requests, and the backup manager needs a reference back to this object to run multitenant scans to build
        // the physical backup off of mt-dynamo data.
        this.backupManager = backupManager.map(b -> b.build(this));
        this.backupTablePrefix = backupTablePrefix;

        this.delegate = new SharedTableMappingDelegate(amazonDynamoDb);
    }

    long getGetRecordsTimeLimit() {
        return getRecordsTimeLimit;
    }

    Clock getClock() {
        return clock;
    }

    @Override
    protected boolean isMtTable(String tableName) {
        return isPhysicalTable(tableName) && !tableName.startsWith(backupTablePrefix);
    }

    private boolean isPhysicalTable(String tableName) {
        return tableMappingFactory.getCreateTableRequestFactory().isPhysicalTable(tableName);
    }

    public MtTableDescriptionRepo getMtTableDescriptionRepo() {
        return mtTableDescriptionRepo;
    }

    Function<Map<String, AttributeValue>, MtContextAndTable> getContextParser(String sharedTableName) {
        return getContextParser(sharedTableName, true);
    }

    private Function<Map<String, AttributeValue>, MtContextAndTable> getContextParser(String sharedTableName,
                                                                                      boolean validateTableName) {
        // skip valid table check if this is invoked by the scanAllTenants(), which either already did the check, or
        // is invoked for backup snapshot tables, which the table mapping factory is not aware of
        if (validateTableName) {
            checkArgument(isPhysicalTable(sharedTableName),
                "Physical table does not belong to this shared table client: %s", sharedTableName);
        }
        DynamoTableDescription table = physicalTableManager.describeTable(sharedTableName);
        checkArgument(table != null);
        String hashKeyName = table.getPrimaryKey().getHashKey();
        ScalarAttributeType hashKeyType = table.getPrimaryKey().getHashKeyType();
        return item -> partitioningStrategy.toContextAndTable(hashKeyType, item.get(hashKeyName));
    }

    @Override
    protected MtAmazonDynamoDbContextProvider getMtContext() {
        return super.getMtContext();
    }

    /**
     * Retrieves batches of items using their primary key.
     */
    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        // validate
        checkArgument(batchGetItemRequest.getRequestItems().keySet().size() == 1,
            "batchGetItem must target one and only one table");

        String unqualifiedTableName = batchGetItemRequest.getRequestItems().keySet().iterator().next();
        TableMapping tableMapping = getTableMapping(unqualifiedTableName).get();

        return delegate.batchGetItem(tableMapping, batchGetItemRequest);
    }

    // TODO move to common class
    static void validateGetItemKeysAndAttribute(KeysAndAttributes keysAndAttributes) {
        checkArgument(keysAndAttributes.getAttributesToGet() == null,
            "attributesToGet are not supported on BatchGetItemRequest calls");
        checkArgument(keysAndAttributes.getProjectionExpression() == null,
            "projectionExpression is not supported on BatchGetItemRequest calls");
        checkArgument(keysAndAttributes.getExpressionAttributeNames() == null,
            "expressionAttributeNames are not supported on BatchGetItemRequest calls");
    }

    /**
     * Create a virtual table configured by {@code createTableRequest}. This does not create a physical table in dynamo,
     * rather it inserts a row into a metadata table, thus creating a virtual table, for the given mt_context tenant to
     * insert data into.
     *
     * @return a {@code CreateTableResult} object with the description of the table created
     */
    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        tableMappingFactory.validateCreateVirtualTableRequest(createTableRequest);
        // TODO: persist virtual to physical secondary index map in repo
        return new CreateTableResult()
            .withTableDescription(withTenantStreamArn(mtTableDescriptionRepo.createTable(createTableRequest)));
    }

    /**
     * Delete a row for the given virtual table configured with {@code deleteItemRequest}.
     *
     * @return a {@code DeleteItemResult} containing the data of the row deleted from dynamo
     */
    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        TableMapping tableMapping = getTableMapping(deleteItemRequest.getTableName()).get();
        return delegate.deleteItem(tableMapping, deleteItemRequest);
    }

    /**
     * Delete the given virtual table for the given mt_context tenant configured with {@code deleteTableRequest}. Note
     * that this is a virtual table, where actual data for the table lives among other tenants' data. Therefore, this
     * command is a relatively (or extraordinarily) expensive operation requiring running a full scan of the shared
     * table to find relevant rows for the given tenant-table pair to delete before table metadata can be deleted.
     *
     * <p>Additionally, for asynchronous deletes, there is no support for this JVM crashing during the delete, in which
     * case, although a successful delete response my be handed back to the client, the virtual table and its relevant
     * data may not actually be properly deleted. Therefore, use with caution.
     *
     * @return a DeleteTableResult with the description of the virtual table deleted
     */
    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        if (deleteTableAsync) {
            Executors.newSingleThreadExecutor().submit(() -> {
                deleteTableInternal(deleteTableRequest);
            });
            return new DeleteTableResult()
                .withTableDescription(mtTableDescriptionRepo.getTableDescription(deleteTableRequest.getTableName()));
        } else {
            return deleteTableInternal(deleteTableRequest);
        }
    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        TableDescription tableDescription =
            mtTableDescriptionRepo.getTableDescription(describeTableRequest.getTableName()).withTableStatus("ACTIVE");
        withTenantStreamArn(tableDescription);
        return new DescribeTableResult().withTable(withTenantStreamArn(tableDescription));
    }

    private TableDescription withTenantStreamArn(TableDescription tableDescription) {
        if (Optional.ofNullable(tableDescription.getStreamSpecification()).map(StreamSpecification::isStreamEnabled)
            .orElse(false)) {
            String arn = getTableMapping(tableDescription.getTableName()).get().getPhysicalTable().getLastStreamArn();
            tableDescription.setLatestStreamArn(
                StreamArn.fromString(arn, getMtContext().getContext(), tableDescription.getTableName()).toString());
        }
        return tableDescription;
    }

    /**
     * Fetch a row by primary key for the given tenant context and virtual table.
     */
    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        // validate
        checkArgument(getItemRequest.getAttributesToGet() == null,
            "attributesToGet are not supported on GetItemRequest calls");
        checkArgument(getItemRequest.getProjectionExpression() == null,
            "projectionExpression is not supported on GetItemRequest calls");
        checkArgument(getItemRequest.getExpressionAttributeNames() == null,
            "expressionAttributeNames are not supported on GetItemRequest calls");

        TableMapping tableMapping = getTableMapping(getItemRequest.getTableName()).get();
        return delegate.getItem(tableMapping, getItemRequest);
    }

    Optional<TableMapping> getTableMapping(String virtualTableName) {
        try {
            return tableMappingCache.get(virtualTableName, () -> {
                try {
                    String context = getMtContext().getContext();
                    return Optional.of(tableMappingFactory.getTableMapping(
                        context,
                        new DynamoTableDescriptionImpl(mtTableDescriptionRepo.getTableDescription(virtualTableName))));
                } catch (ResourceNotFoundException e) {
                    // This isn't great, but we're assuming a missing virtual table entry here is a deleted virtual
                    // table, and thus, shouldn't be mapping records to MtRecords. Instead return null, but really,
                    // we should make sure we let deleted records flow through a stream and expire out, before removing
                    // the virtual table entry.
                    return Optional.empty();
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("exception mapping virtual table " + virtualTableName, e);
        }
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        // validate scan attributes are not populated
        checkArgument(!putItemRequest.getItem().containsKey(scanTenantKey),
            "Trying to update a reserved column name: " + scanTenantKey);
        checkArgument(!putItemRequest.getItem().containsKey(scanVirtualTableKey),
            "Trying to update a reserved column name: " + scanVirtualTableKey);

        TableMapping tableMapping = getTableMapping(putItemRequest.getTableName()).get();
        return delegate.putItem(tableMapping, putItemRequest);
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        final TableMapping tableMapping = getTableMapping(queryRequest.getTableName()).get();
        return delegate.query(tableMapping, queryRequest);
    }

    /**
     * Execute a scan meeting the specs of {@link AmazonDynamoDB}, but scoped to single tenants within a shared table.
     * If no tenant context is specified, a multitenant scan is performed over the multitenant shared table. Scans
     * scoped to single tenants are inefficient. Multitenant scans are as bad as scans on any other dynamo table, i.e.,
     * not great.
     *
     * <p>This should rarely, if ever, be exposed for tenants to run, given how expensive scans on a shared table are.
     * If used, it needs to be on a non-web request, since this makes several repeat callouts to dynamo to fill a single
     * result set. Performance of this call degrades with data size of all other tenant-table data stored, and will
     * likely time out a synchronous web request if querying a sparse table-tenant in the shared table.
     */
    @Override
    public ScanResult scan(ScanRequest scanRequest) {
        if (getMtContext().getContextOpt().isEmpty()) {
            // if we are here, we are doing a multitenant scan on a shared table
            checkArgument(isPhysicalTable(scanRequest.getTableName()),
                "Non-context scan called for an invalid physical table: %s", scanRequest.getTableName());
            return scanAllTenants(scanRequest);
        }

        TableMapping tableMapping = getTableMapping(scanRequest.getTableName()).get();
        return delegate.scan(tableMapping, scanRequest);
    }

    private ScanResult scanAllTenants(ScanRequest scanRequest) {
        ScanResult scanResult = getAmazonDynamoDb().scan(scanRequest);

        // given the shared table we are working with,
        // get the function to map the primary key back to
        // tuple (tenant, virtual table name, primary attributes)
        Function<Map<String, AttributeValue>, MtContextAndTable> contextParser =
            getContextParser(scanRequest.getTableName(), false);
        List<Map<String, AttributeValue>> unpackedItems = new ArrayList<>(scanResult.getItems().size());
        for (Map<String, AttributeValue> item : scanResult.getItems()) {
            // go through each row in the scan and pull out the tenant and table information from the primary key to
            // separate attributes in each item's map
            MtContextAndTable contextAndTable = contextParser.apply(item);
            TableMapping tableMapping = getMtContext().withContext(contextAndTable.getContext(),
                this::getTableMapping, contextAndTable.getTableName()).get();
            Map<String, AttributeValue> unpackedVirtualItem = tableMapping.getItemMapper().reverse(item);
            unpackedVirtualItem.put(scanTenantKey, new AttributeValue(contextAndTable.getContext()));
            unpackedVirtualItem.put(scanVirtualTableKey, new AttributeValue(contextAndTable.getTableName()));
            unpackedItems.add(unpackedVirtualItem);
        }

        return scanResult.withItems(unpackedItems);
    }

    // TODO move to common class
    static boolean projectionContainsKey(ScanRequest request, PrimaryKey key) {
        String projection = request.getProjectionExpression();
        List<String> legacyProjection = request.getAttributesToGet();

        // vacuously true if projection not specified
        if (projection == null && legacyProjection == null) {
            return true;
        } else {
            Map<String, String> expressionNames = request.getExpressionAttributeNames();
            return projectionContainsKey(projection, expressionNames, legacyProjection, key.getHashKey()) && key
                .getRangeKey()
                .map(rangeKey -> projectionContainsKey(projection, expressionNames, legacyProjection, rangeKey))
                .orElse(true);
        }
    }

    private static boolean projectionContainsKey(String projection, Map<String, String> expressionNames,
                                                 List<String> legacyProjection, String key) {
        if (projection != null) {
            // TODO we should probably parse expressions or use more sophisticated matching
            if (expressionNames != null) {
                String name = expressionNames.get(key);
                if (name != null && projection.contains(name)) {
                    return true;
                }
            }
            if (projection.contains(key)) {
                return true;
            }
        }
        return legacyProjection != null && legacyProjection.contains(key);
    }

    /**
     * Update a given row with primary key defined in {@code updateItemRequest}.
     *
     * @return {@code UpdateItemResult} of the updated row
     */
    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        // validate scan attributes are not being used
        if (updateItemRequest.getExpressionAttributeNames() != null) {
            checkArgument(
                !updateItemRequest.getExpressionAttributeNames().containsKey(scanTenantKey),
                "Trying to update a reserved column name: " + scanTenantKey);
            checkArgument(
                !updateItemRequest.getExpressionAttributeNames().containsKey(scanVirtualTableKey),
                "Trying to update a reserved column name: " + scanVirtualTableKey);
        }

        TableMapping tableMapping = getTableMapping(updateItemRequest.getTableName()).get();
        return delegate.updateItem(tableMapping, updateItemRequest);
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName, Integer limit) {
        if (getMtContext().getContextOpt().isEmpty()) {
            // delegate to parent class' multitenant listTables call if no tenant is provided
            return super.listTables(exclusiveStartTableName, limit);
        } else {
            return mtTableDescriptionRepo.listTables(new ListTablesRequest()
                .withExclusiveStartTableName(exclusiveStartTableName)
                .withLimit(limit));
        }
    }

    @Override
    public ListBackupsResult listBackups(ListBackupsRequest listBackupsRequest) {
        if (backupManager.isPresent()) {
            if (getMtContext().getContextOpt().isPresent()) {
                return backupManager.get().listTenantTableBackups(listBackupsRequest, getMtContext().getContext());
            }
            return backupManager.get().listBackups(listBackupsRequest);
        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    @Override
    public RestoreTableFromBackupResult restoreTableFromBackup(
        RestoreTableFromBackupRequest restoreTableFromBackupRequest) {
        if (backupManager.isPresent()) {
            if (getMtContext().getContextOpt().isEmpty()) {
                throw new MtBackupException("Cannot do restore of backup without tenant specifier", null);
            }

            TenantTableBackupMetadata backupMetadata = backupManager.get()
                .getTenantTableBackupFromArn(restoreTableFromBackupRequest.getBackupArn());
            RestoreMtBackupRequest mtRestoreRequest = new RestoreMtBackupRequest(backupMetadata.getBackupName(),
                new TenantTable(backupMetadata.getVirtualTableName(), backupMetadata.getTenantId()),
                new TenantTable(restoreTableFromBackupRequest.getTargetTableName(), getMtContext().getContext()));
            TenantRestoreMetadata restoreMetadata = backupManager.get().restoreTenantTableBackup(
                mtRestoreRequest,
                getMtContext());

            return new RestoreTableFromBackupResult().withTableDescription(
                mtTableDescriptionRepo.getTableDescription(restoreMetadata.getVirtualTableName()));
        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    @Override
    public DescribeBackupResult describeBackup(DescribeBackupRequest describeBackupRequest) {
        if (backupManager.isPresent()) {
            if (getMtContext().getContextOpt().isPresent()) {
                // tenant specific describe backup
                TenantTableBackupMetadata tenantBackupMetadata = backupManager.get()
                    .getTenantTableBackupFromArn(describeBackupRequest.getBackupArn());
                TenantBackupMetadata backupMetadata =
                    backupManager.get().getTenantTableBackup(tenantBackupMetadata.getBackupName(),
                        new TenantTable(
                            tenantBackupMetadata.getVirtualTableName(), tenantBackupMetadata.getTenantId()));
                if (backupMetadata != null) {
                    return MtBackupAwsAdaptorKt.getBackupAdaptorSingleton().getDescribeBackupResult(backupMetadata);
                } else {
                    throw new BackupNotFoundException("No backup with arn "
                        + describeBackupRequest.getBackupArn() + " found");
                }
            } else {
                //multi tenant backup describe
                MtBackupMetadata backupMetadata = getBackupManager().getBackup(describeBackupRequest.getBackupArn());
                if (backupMetadata != null) {
                    return MtBackupAwsAdaptorKt.getBackupAdaptorSingleton().getDescribeBackupResult(backupMetadata);
                } else {
                    throw new BackupNotFoundException("No backup with arn "
                        + describeBackupRequest.getBackupArn() + "found");
                }
            }
        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }


    @Override
    public DeleteBackupResult deleteBackup(DeleteBackupRequest deleteBackupRequest) {
        if (backupManager.isPresent()) {
            checkArgument(getMtContext().getContextOpt().isEmpty(),
                "Cannot delete tenant scoped backup");
            Preconditions.checkNotNull(deleteBackupRequest.getBackupArn(), "Must pass backup arn.");
            MtBackupMetadata backupMetadata = backupManager.get().deleteBackup(deleteBackupRequest.getBackupArn());
            if (backupMetadata != null) {
                BackupDescription backupDescription = MtBackupAwsAdaptorKt.getBackupAdaptorSingleton()
                    .getBackupDescription(backupMetadata);
                return new DeleteBackupResult()
                    .withBackupDescription(backupDescription);
            } else {
                throw new BackupNotFoundException("No backup with given ARN found");
            }
        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    @Override
    public CreateBackupResult createBackup(CreateBackupRequest createBackupRequest) {
        if (backupManager.isPresent()) {
            Preconditions.checkNotNull(createBackupRequest.getBackupName(), "Must pass backup name.");
            checkArgument(createBackupRequest.getTableName() == null,
                "Multitenant backups cannot backup individual tables, table-name arguments are disallowed");
            backupManager.get().createBackup(createBackupRequest);

            // list all physical tables belonging to this shared table client
            Collection<String> origPhysicalTables = new AmazonDynamoDbAdminUtils(getAmazonDynamoDb())
                .listTables(this::isPhysicalTable);

            ExecutorService executorService = Executors.newFixedThreadPool(origPhysicalTables.size());

            String backupRequestTablePrefix = getSnapshotBackupRequestTablePrefix(createBackupRequest);
            List<Future<SnapshotResult>> futures = new ArrayList<>();
            for (String tableName : origPhysicalTables) {
                String snapshottedTable = backupRequestTablePrefix + tableName;
                futures.add(executorService.submit(
                    snapshotScanAndBackup(createBackupRequest, tableName, snapshottedTable)));
            }
            List<SnapshotResult> snapshotResults = new ArrayList<>();
            try {
                for (Future<SnapshotResult> future : futures) {
                    try {
                        snapshotResults.add(future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new MtBackupException("Error snapshotting table", e);
                    }
                }
            } finally {
                executorService.shutdown();
                for (SnapshotResult result : snapshotResults) {
                    backupManager.get().getMtBackupTableSnapshotter().cleanup(result, getAmazonDynamoDb());
                }
            }

            MtBackupMetadata finishedMetadata = backupManager.get().markBackupComplete(createBackupRequest);
            return new CreateBackupResult().withBackupDetails(
                new BackupDetails()
                    .withBackupArn(finishedMetadata.getMtBackupName())
                    .withBackupCreationDateTime(new Date(finishedMetadata.getCreationTime()))
                    .withBackupName(finishedMetadata.getMtBackupName()));

        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    private Callable<SnapshotResult> snapshotScanAndBackup(CreateBackupRequest createBackupRequest, String tableName,
                                                           String snapshottedTable) {
        return () -> {
            SnapshotResult snapshotResult = backupManager.get()
                .getMtBackupTableSnapshotter()
                .snapshotTableToTarget(
                    new SnapshotRequest(createBackupRequest.getBackupName(),
                        tableName,
                        snapshottedTable,
                        getAmazonDynamoDb(),
                        new ProvisionedThroughput(10L, 10L))
                );
            backupManager.get().getMtBackupTableSnapshotter();
            backupManager.get().backupPhysicalMtTable(createBackupRequest,
                snapshotResult.getTempSnapshotTable());
            return snapshotResult;
        };
    }

    /**
     * Used by {@link MtSharedTableBackupManager#createBackupData}.
     */
    ScanResult scanBackupSnapshotTable(CreateBackupRequest createBackupRequest, ScanRequest scanRequest) {
        final String backupRequestTablePrefix = getSnapshotBackupRequestTablePrefix(createBackupRequest);
        checkArgument(scanRequest.getTableName().startsWith(backupRequestTablePrefix),
            "Scan not called on a snapshot table of backup request (CreateBackupRequest: %s. Table: %s)",
            createBackupRequest.getBackupName(), scanRequest.getTableName());
        checkArgument(isPhysicalTable(scanRequest.getTableName().substring(backupRequestTablePrefix.length())),
            "Scan not called on a snapshot table belonging to this client: %s",
            scanRequest.getTableName());
        return scanAllTenants(scanRequest);
    }

    private String getSnapshotBackupRequestTablePrefix(CreateBackupRequest createBackupRequest) {
        return backupTablePrefix + createBackupRequest.getBackupName() + ".";
    }

    /**
     * When doing multitenant scans on a shared table, this column name is injected into each row's scan result to
     * add tenant context to each scan result. This is mt-dynamo client overrideable, and returns the value the
     * client was built with.
     */
    public String getMtScanTenantKey() {
        return scanTenantKey;
    }

    /**
     * When doing multitenant scans on a shared table, this column name is injected into each row's scan result to
     * add tenant-table context to each scan result. This is mt-dynamo client overrideable, and returns the value the
     * client was built with.
     */
    public String getMtScanVirtualTableKey() {
        return scanVirtualTableKey;
    }


    /**
     * In order to generate tenant table granular virtual table snapshots, a temp table is used to snapshot all
     * physical tables. These snapshot tables are prefixed with this client overrideable string.
     */
    public String getBackupTablePrefix() {
        return backupTablePrefix;
    }

    /**
     * See class-level Javadoc for explanation of why the use of {@code addAttributeUpdateEntry} and
     * {@code withAttributeUpdates} is not supported.
     */
    // TODO move to common class
    static void validateUpdateItemRequest(UpdateItemRequest updateItemRequest) {
        checkArgument(updateItemRequest.getAttributeUpdates() == null,
            "Use of attributeUpdates in UpdateItemRequest objects is not supported.  Use UpdateExpression instead.");
    }

    @Override
    public String toString() {
        return name;
    }

    private DeleteTableResult deleteTableInternal(DeleteTableRequest deleteTableRequest) {
        String tableDesc = "table=" + deleteTableRequest.getTableName() + " " + (deleteTableAsync ? "asynchronously"
            : "synchronously");
        log.warn("dropping " + tableDesc);
        truncateTable(deleteTableRequest.getTableName());
        DeleteTableResult deleteTableResult = new DeleteTableResult()
            .withTableDescription(mtTableDescriptionRepo.deleteTable(deleteTableRequest.getTableName()));
        log.warn("dropped " + tableDesc);
        return deleteTableResult;
    }

    private void truncateTable(String tableName) {
        if (truncateOnDeleteTable) {
            TableMapping tableMapping = getTableMapping(tableName).get();
            long deletedCount = delegate.truncateTable(tableMapping);
            log.warn("truncation of " + deletedCount + " items from table=" + tableName + " complete.");
        } else {
            log.info("truncateOnDeleteTable is disabled for " + tableName + ", skipping truncation. "
                + "Data has been dropped, clean up on aisle "
                + getMtContext().getContextOpt().orElse("no-context"));
        }
    }

    private Map<String, AttributeValue> getKeyFromItem(Map<String, AttributeValue> item, String tableName) {
        return describeTable(new DescribeTableRequest().withTableName(tableName)).getTable().getKeySchema().stream()
            .collect(Collectors.toMap(KeySchemaElement::getAttributeName,
                keySchemaElement -> item.get(keySchemaElement.getAttributeName())));
    }

    // TODO move to common class
    static Map<String, AttributeValue> getKeyFromItem(Map<String, AttributeValue> item, PrimaryKey primaryKey) {
        String hashKey = primaryKey.getHashKey();
        return primaryKey.getRangeKey()
            .map(rangeKey -> ImmutableMap.of(hashKey, item.get(hashKey), rangeKey, item.get(rangeKey)))
            .orElseGet(() -> ImmutableMap.of(hashKey, item.get(hashKey)));
    }

    @VisibleForTesting MtBackupManager getBackupManager() {
        return backupManager.orElse(null);
    }

    // TODO move to common class
    static class PutItemRequestWrapper extends AbstractRequestWrapper {

        PutItemRequestWrapper(PutItemRequest putItemRequest) {
            super(putItemRequest::getExpressionAttributeNames, putItemRequest::setExpressionAttributeNames,
                putItemRequest::getExpressionAttributeValues, putItemRequest::setExpressionAttributeValues,
                putItemRequest::getConditionExpression, putItemRequest::setConditionExpression);
        }
    }

    // TODO move to common class
    static class DeleteItemRequestWrapper extends AbstractRequestWrapper {

        DeleteItemRequestWrapper(DeleteItemRequest deleteItemRequest) {
            super(deleteItemRequest::getExpressionAttributeNames, deleteItemRequest::setExpressionAttributeNames,
                deleteItemRequest::getExpressionAttributeValues, deleteItemRequest::setExpressionAttributeValues,
                deleteItemRequest::getConditionExpression, deleteItemRequest::setConditionExpression);
        }
    }

}