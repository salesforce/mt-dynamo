/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BackupDetails;
import com.amazonaws.services.dynamodbv2.model.BackupNotFoundException;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ContinuousBackupsUnavailableException;
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest;
import com.amazonaws.services.dynamodbv2.model.CreateBackupResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
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
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.backups.CreateMtBackupRequest;
import com.salesforce.dynamodbv2.mt.backups.MtBackupAwsAdaptorKt;
import com.salesforce.dynamodbv2.mt.backups.MtBackupException;
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager;
import com.salesforce.dynamodbv2.mt.backups.MtBackupMetadata;
import com.salesforce.dynamodbv2.mt.backups.RestoreMtBackupRequest;
import com.salesforce.dynamodbv2.mt.backups.TenantRestoreMetadata;
import com.salesforce.dynamodbv2.mt.cache.MtCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
    private final Cache<Object, TableMapping> tableMappingCache;
    private final TableMappingFactory tableMappingFactory;
    private final boolean deleteTableAsync;
    private final boolean truncateOnDeleteTable;
    private final Map<String, CreateTableRequest> mtTables;
    private final long getRecordsTimeLimit;
    private final Clock clock;
    private final String scanTenantKey;
    private final String scanVirtualTableKey;
    private final Optional<MtBackupManager> backupManager;

    /**
     * Shared-table constructor.
     *
     * @param name                   the name of the multitenant AmazonDynamoDB instance
     * @param mtContext              the multitenant context provider
     * @param amazonDynamoDb         the underlying {@code AmazonDynamoDB} delegate
     * @param tableMappingFactory    the table-mapping factory for mapping virtual to physical table instances
     * @param mtTableDescriptionRepo the {@code MtTableDescriptionRepo} impl
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
                                         MtTableDescriptionRepo mtTableDescriptionRepo,
                                         boolean deleteTableAsync,
                                         boolean truncateOnDeleteTable,
                                         long getRecordsTimeLimit,
                                         Clock clock,
                                         Cache<Object, TableMapping> tableMappingCache,
                                         MeterRegistry meterRegistry,
                                         Optional<MtSharedTableBackupManagerBuilder> backupManager,
                                         String scanTenantKey,
                                         String scanVirtualTableKey) {
        super(mtContext, amazonDynamoDb, meterRegistry);
        this.name = name;
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        this.tableMappingCache = new MtCache<>(mtContext, tableMappingCache);
        this.tableMappingFactory = tableMappingFactory;
        this.deleteTableAsync = deleteTableAsync;
        this.truncateOnDeleteTable = truncateOnDeleteTable;
        this.mtTables = tableMappingFactory.getCreateTableRequestFactory().getPhysicalTables().stream()
            .collect(Collectors.toMap(CreateTableRequest::getTableName, Function.identity()));
        this.getRecordsTimeLimit = getRecordsTimeLimit;
        this.clock = clock;
        this.scanTenantKey = scanTenantKey;
        this.scanVirtualTableKey = scanVirtualTableKey;
        this.backupManager = backupManager.map(b -> b.build(this));
    }

    long getGetRecordsTimeLimit() {
        return getRecordsTimeLimit;
    }

    Clock getClock() {
        return clock;
    }

    @Override
    protected boolean isMtTable(String tableName) {
        return mtTables.containsKey(tableName);
    }

    public MtTableDescriptionRepo getMtTableDescriptionRepo() {
        return mtTableDescriptionRepo;
    }

    Function<Map<String, AttributeValue>, FieldValue<?>> getFieldValueFunction(String sharedTableName) {
        CreateTableRequest table = mtTables.get(sharedTableName);
        checkArgument(table != null);
        // TODO consider representing physical tables as DynamoTableDescription
        String hashKeyName = table.getKeySchema().stream()
            .filter(elem -> HASH.toString().equals(elem.getKeyType()))
            .map(KeySchemaElement::getAttributeName)
            .findFirst().orElseThrow(IllegalStateException::new);
        ScalarAttributeType hashKeyType = table.getAttributeDefinitions().stream()
            .filter(attr -> hashKeyName.equals(attr.getAttributeName()))
            .map(AttributeDefinition::getAttributeType)
            .map(ScalarAttributeType::valueOf)
            .findFirst().orElseThrow(IllegalStateException::new);
        switch (hashKeyType) {
            case S:
                return key -> StringFieldPrefixFunction.INSTANCE.reverse(key.get(hashKeyName).getS());
            case B:
                return key -> BinaryFieldPrefixFunction.INSTANCE.reverse(key.get(hashKeyName).getB());
            default:
                throw new IllegalStateException("Unsupported physical table hash key type " + hashKeyType);
        }
    }

    @Override
    protected MtAmazonDynamoDbContextProvider getMtContext() {
        return super.getMtContext();
    }

    /**
     * Retrieves batches of items using their primary key.
     */
    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest unqualifiedBatchGetItemRequest) {
        // validate
        unqualifiedBatchGetItemRequest.getRequestItems().values()
            .forEach(MtAmazonDynamoDbBySharedTable::validateGetItemKeysAndAttribute);

        // clone request and clear items
        Map<String, KeysAndAttributes> unqualifiedKeysByTable = unqualifiedBatchGetItemRequest.getRequestItems();
        BatchGetItemRequest qualifiedBatchGetItemRequest = unqualifiedBatchGetItemRequest.clone();
        qualifiedBatchGetItemRequest.clearRequestItemsEntries();

        // create a map of physical table names to TableMapping for use when handling the request later
        Map<String, TableMapping> tableMappingByPhysicalTableName = new HashMap<>();

        // for each table in the batch request, map table name and keys
        unqualifiedKeysByTable.forEach((unqualifiedTableName, unqualifiedKeys) -> {
            // map table name
            TableMapping tableMapping = getTableMapping(unqualifiedTableName);
            String qualifiedTableName = tableMapping.getPhysicalTable().getTableName();
            tableMappingByPhysicalTableName.put(qualifiedTableName, tableMapping);
            // map key
            qualifiedBatchGetItemRequest.addRequestItemsEntry(
                qualifiedTableName,
                new KeysAndAttributes().withKeys(unqualifiedKeys.getKeys().stream().map(
                    key -> tableMapping.getItemMapper().apply(key)).collect(Collectors.toList())));
        });

        // batch get
        final BatchGetItemResult qualifiedBatchGetItemResult = getAmazonDynamoDb()
            .batchGetItem(qualifiedBatchGetItemRequest);
        Map<String, List<Map<String, AttributeValue>>> qualifiedItemsByTable = qualifiedBatchGetItemResult
            .getResponses();

        // map result
        final BatchGetItemResult unqualifiedBatchGetItemResult = qualifiedBatchGetItemResult.clone();
        unqualifiedBatchGetItemResult.clearResponsesEntries();
        qualifiedItemsByTable.forEach((qualifiedTableName, qualifiedItems) -> {
            TableMapping tableMapping = tableMappingByPhysicalTableName.get(qualifiedTableName);
            unqualifiedBatchGetItemResult.addResponsesEntry(
                tableMapping.getVirtualTable().getTableName(),
                qualifiedItems.stream().map(keysAndAttributes ->
                    tableMapping.getItemMapper().reverse(keysAndAttributes)).collect(Collectors.toList()));
            // map unprocessedKeys
            if (!qualifiedBatchGetItemResult.getUnprocessedKeys().isEmpty()) {
                unqualifiedBatchGetItemResult.clearUnprocessedKeysEntries();
                qualifiedBatchGetItemResult.getUnprocessedKeys()
                    .forEach((qualifiedTableNameUk, qualifiedUkKeysAndAttributes) -> {
                        TableMapping tableMappingUk = tableMappingByPhysicalTableName.get(qualifiedTableNameUk);
                        unqualifiedBatchGetItemResult.addUnprocessedKeysEntry(
                            tableMappingUk.getVirtualTable().getTableName(),
                            new KeysAndAttributes()
                                .withConsistentRead(qualifiedUkKeysAndAttributes.getConsistentRead())
                                .withKeys(qualifiedUkKeysAndAttributes.getKeys().stream()
                                    .map(keysAndAttributes ->
                                        tableMapping.getKeyMapper().reverse(keysAndAttributes))
                                    .collect(Collectors.toList())));
                    });
            }
        });

        return unqualifiedBatchGetItemResult;
    }

    private static void validateGetItemKeysAndAttribute(KeysAndAttributes keysAndAttributes) {
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
        // map table name
        deleteItemRequest = deleteItemRequest.clone();
        TableMapping tableMapping = getTableMapping(deleteItemRequest.getTableName());
        deleteItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        deleteItemRequest.setKey(tableMapping.getItemMapper().apply(deleteItemRequest.getKey()));

        // map conditions
        tableMapping.getConditionMapper().apply(new DeleteItemRequestWrapper(deleteItemRequest));

        // delete
        return getAmazonDynamoDb().deleteItem(deleteItemRequest);
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
            String arn = getTableMapping(tableDescription.getTableName()).getPhysicalTable().getLastStreamArn();
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

        // map table name
        getItemRequest = getItemRequest.clone();
        TableMapping tableMapping = getTableMapping(getItemRequest.getTableName());
        getItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        getItemRequest.setKey(tableMapping.getKeyMapper().apply(getItemRequest.getKey()));

        // get
        GetItemResult getItemResult = getAmazonDynamoDb().getItem(getItemRequest);

        // map result
        if (getItemResult.getItem() != null) {
            getItemResult.withItem(tableMapping.getItemMapper().reverse(getItemResult.getItem()));
        }

        return getItemResult;
    }

    TableMapping getTableMapping(String virtualTableName) {
        try {
            return tableMappingCache.get(virtualTableName, () ->
                tableMappingFactory.getTableMapping(
                    new DynamoTableDescriptionImpl(mtTableDescriptionRepo.getTableDescription(virtualTableName))));
        } catch (ExecutionException e) {
            throw new RuntimeException("exception mapping virtual table " + virtualTableName, e);
        }
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        // map table name
        putItemRequest = putItemRequest.clone();
        TableMapping tableMapping = getTableMapping(putItemRequest.getTableName());
        putItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map conditions
        tableMapping.getConditionMapper().apply(new PutItemRequestWrapper(putItemRequest));

        // map item
        putItemRequest.setItem(tableMapping.getItemMapper().apply(putItemRequest.getItem()));

        // put
        return getAmazonDynamoDb().putItem(putItemRequest);
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        final TableMapping tableMapping = getTableMapping(queryRequest.getTableName());

        // map table name
        final QueryRequest clonedQueryRequest = queryRequest.clone();
        clonedQueryRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map query request
        tableMapping.getQueryAndScanMapper().apply(clonedQueryRequest);

        // map result
        final QueryResult queryResult = getAmazonDynamoDb().query(clonedQueryRequest);
        queryResult.setItems(queryResult.getItems().stream().map(tableMapping.getItemMapper()::reverse)
            .collect(toList()));
        if (queryResult.getLastEvaluatedKey() != null) {
            queryResult.setLastEvaluatedKey(tableMapping.getItemMapper().reverse(queryResult.getLastEvaluatedKey()));
        }

        return queryResult;
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
            return scanAllTenants(scanRequest);
        }
        TableMapping tableMapping = getTableMapping(scanRequest.getTableName());
        PrimaryKey key = scanRequest.getIndexName() == null ? tableMapping.getVirtualTable().getPrimaryKey()
            : tableMapping.getVirtualTable().findSi(scanRequest.getIndexName()).getPrimaryKey();

        // Projection must include primary key, since we use it for paging.
        // (We could add key fields into projection and filter result in the future)
        checkArgument(projectionContainsKey(scanRequest, key),
            "Multitenant scans must include key in projection expression");

        // map table name
        ScanRequest clonedScanRequest = scanRequest.clone();
        clonedScanRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map scan request
        clonedScanRequest.setExpressionAttributeNames(Optional.ofNullable(clonedScanRequest.getFilterExpression())
            .map(s -> new HashMap<>(clonedScanRequest.getExpressionAttributeNames())).orElseGet(HashMap::new));
        clonedScanRequest.setExpressionAttributeValues(Optional.ofNullable(clonedScanRequest.getFilterExpression())
            .map(s -> new HashMap<>(clonedScanRequest.getExpressionAttributeValues())).orElseGet(HashMap::new));
        tableMapping.getQueryAndScanMapper().apply(clonedScanRequest);

        // keep moving forward pages until we find at least one record for current tenant or reach end
        ScanResult scanResult;
        while ((scanResult = getAmazonDynamoDb().scan(clonedScanRequest)).getItems().isEmpty()
            && scanResult.getLastEvaluatedKey() != null) {
            clonedScanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }

        // map result
        List<Map<String, AttributeValue>> items = scanResult.getItems();
        if (!items.isEmpty()) {
            scanResult.setItems(items.stream().map(tableMapping.getItemMapper()::reverse).collect(toList()));
            if (scanResult.getLastEvaluatedKey() != null) {
                scanResult.setLastEvaluatedKey(getKeyFromItem(Iterables.getLast(scanResult.getItems()), key));
            }
        } // else: while loop ensures that getLastEvaluatedKey is null (no need to map)

        return scanResult;
    }

    private ScanResult scanAllTenants(ScanRequest scanRequest) {
        Preconditions.checkArgument(mtTables.containsKey(scanRequest.getTableName()), scanRequest.getTableName());
        ScanResult scanResult = getAmazonDynamoDb().scan(scanRequest);

        // given the shared table we are working with,
        // get the function to map the primary key back to
        // tuple (tenant, virtual table name, primary attributes)
        Function<Map<String, AttributeValue>, FieldValue<?>> fieldMapperFunction =
            getFieldValueFunction(scanRequest.getTableName());
        List<Map<String, AttributeValue>> unpackedItems = new ArrayList<>(scanResult.getItems().size());
        for (Map<String, AttributeValue> item : scanResult.getItems()) {
            // go through each row in the scan and pull out the tenant and table information from the primary key to
            // separate attributes in each item's map
            FieldValue virtualFieldKeys = fieldMapperFunction.apply(item);
            TableMapping tableMapping = getMtContext().withContext(virtualFieldKeys.getContext(), this::getTableMapping,
                virtualFieldKeys.getTableName());
            Map<String, AttributeValue> unpackedVirtualItem = tableMapping.getItemMapper().reverse(item);
            unpackedVirtualItem.put(scanTenantKey, new AttributeValue(virtualFieldKeys.getContext()));
            unpackedVirtualItem.put(scanVirtualTableKey, new AttributeValue(virtualFieldKeys.getTableName()));
            unpackedItems.add(unpackedVirtualItem);
        }

        return scanResult.withItems(unpackedItems);
    }

    @VisibleForTesting
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
        // validate that attributeUpdates are not being used
        validateUpdateItemRequest(updateItemRequest);

        // map table name
        updateItemRequest = updateItemRequest.clone();
        TableMapping tableMapping = getTableMapping(updateItemRequest.getTableName());
        updateItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        updateItemRequest.setKey(tableMapping.getItemMapper().apply(updateItemRequest.getKey()));

        // map conditions
        tableMapping.getConditionMapper().apply(new UpdateItemRequestWrapper(updateItemRequest));

        // update
        return getAmazonDynamoDb().updateItem(updateItemRequest);
    }

    @Override
    public RestoreTableFromBackupResult restoreTableFromBackup(
        RestoreTableFromBackupRequest restoreTableFromBackupRequest) {
        if (backupManager.isPresent()) {
            if (!(restoreTableFromBackupRequest instanceof RestoreMtBackupRequest)) {
                throw new ContinuousBackupsUnavailableException("Unexpected restore request found, must be an instance"
                    + " of MtRestoreTableFromBackupRequest.");
            }
            TenantRestoreMetadata restoreMetadata = backupManager.get().restoreTenantTableBackup(
                (RestoreMtBackupRequest)restoreTableFromBackupRequest,
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
                throw new UnsupportedOperationException("TODO: Implement tenant table backup describe");
            }
            MtBackupMetadata backupMetadata = getBackupManager().getBackup(describeBackupRequest.getBackupArn());
            if (backupMetadata != null) {
                return MtBackupAwsAdaptorKt.getBackupAdaptorSingleton().getDescribeBackupResult(backupMetadata);
            } else {
                throw new BackupNotFoundException("No backup with arn "
                    + describeBackupRequest.getBackupArn() + "found");
            }
        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    /**
     * TODO: Make this an async operation that properly uses an on-demand dynamo backup/restored table to run scans
     * against with a callback hook to monitor progress and wait for backup completion like the existing dynamo backup
     * APIs. For now, this is a synchronous call that operates the scan against all live table that may be taking
     * writes, so proceed with caution.
     */
    @Override
    public CreateBackupResult createBackup(CreateBackupRequest createBackupRequest) {
        if (backupManager.isPresent()) {
            if (!(createBackupRequest instanceof CreateMtBackupRequest)) {
                throw new MtBackupException("Create instance of {@link CreateMtBackupRequest} required");
            }
            CreateMtBackupRequest createMtBackupRequest = (CreateMtBackupRequest)createBackupRequest;
            backupManager.get().createMtBackup(createMtBackupRequest);

            for (String tableName : mtTables.keySet()) {
                backupManager.get().backupPhysicalMtTable(createMtBackupRequest, tableName);
            }
            MtBackupMetadata finishedMetadata = backupManager.get().markBackupComplete(createMtBackupRequest);
            return new CreateBackupResult().withBackupDetails(
                new BackupDetails()
                    // TODO: maybe this should be the ARN for the global S3 metadata file for this backup
                    .withBackupArn(finishedMetadata.getMtBackupName())
                    .withBackupCreationDateTime(new Date(finishedMetadata.getCreationTime()))
                    .withBackupName(finishedMetadata.getMtBackupName()));

        } else {
            throw new ContinuousBackupsUnavailableException("Backups can only be created by configuring a backup "
                + "managed on an mt-dynamo table builder, see <insert link to backup guide>");
        }
    }

    /**
     * See class-level Javadoc for explanation of why the use of {@code addAttributeUpdateEntry} and
     * {@code withAttributeUpdates} is not supported.
     */
    private static void validateUpdateItemRequest(UpdateItemRequest updateItemRequest) {
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
            ScanResult scanResult = scan(new ScanRequest().withTableName(tableName));
            log.warn("truncating " + scanResult.getItems().size() + " items from table=" + tableName);
            for (Map<String, AttributeValue> item : scanResult.getItems()) {
                deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(getKeyFromItem(item, tableName)));
            }
            log.warn("truncation of " + scanResult.getItems().size() + " items from table=" + tableName
                + (scanResult.getLastEvaluatedKey() == null
                ? " complete. "
                : "but data may have been dropped to the floor, beware."));

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

    private static Map<String, AttributeValue> getKeyFromItem(Map<String, AttributeValue> item, PrimaryKey primaryKey) {
        String hashKey = primaryKey.getHashKey();
        return primaryKey.getRangeKey()
            .map(rangeKey -> ImmutableMap.of(hashKey, item.get(hashKey), rangeKey, item.get(rangeKey)))
            .orElseGet(() -> ImmutableMap.of(hashKey, item.get(hashKey)));
    }

    @VisibleForTesting MtBackupManager getBackupManager() {
        return backupManager.orElse(null);
    }

    private static class PutItemRequestWrapper implements RequestWrapper {

        private final PutItemRequest putItemRequest;

        PutItemRequestWrapper(PutItemRequest putItemRequest) {
            this.putItemRequest = putItemRequest;
            if (this.putItemRequest.getExpressionAttributeNames() != null) {
                this.putItemRequest.setExpressionAttributeNames(new HashMap<>(this.getExpressionAttributeNames()));
            }
            if (this.putItemRequest.getExpressionAttributeValues() != null) {
                this.putItemRequest.setExpressionAttributeValues(new HashMap<>(this.getExpressionAttributeValues()));
            }
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return putItemRequest.getExpressionAttributeNames();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (putItemRequest.getExpressionAttributeNames() == null) {
                putItemRequest.setExpressionAttributeNames(new HashMap<>());
            }
            putItemRequest.getExpressionAttributeNames().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            if (putItemRequest.getExpressionAttributeValues() == null) {
                putItemRequest.setExpressionAttributeValues(new HashMap<>());
            }
            return putItemRequest.getExpressionAttributeValues();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            putItemRequest.getExpressionAttributeValues().put(key, value);
        }

        @Override
        public String getPrimaryExpression() {
            return putItemRequest.getConditionExpression();
        }

        @Override
        public void setPrimaryExpression(String expression) {
            putItemRequest.setConditionExpression(expression);
        }

        @Override
        public String getFilterExpression() {
            return putItemRequest.getConditionExpression();
        }

        @Override
        public void setFilterExpression(String conditionalExpression) {
            putItemRequest.setConditionExpression(conditionalExpression);
        }

        @Override
        public String getIndexName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIndexName(String indexName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Condition> getLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            throw new UnsupportedOperationException();
        }

    }

    private static class UpdateItemRequestWrapper implements RequestWrapper {

        private final UpdateItemRequest updateItemRequest;

        UpdateItemRequestWrapper(UpdateItemRequest updateItemRequest) {
            this.updateItemRequest = updateItemRequest;
            if (this.updateItemRequest.getExpressionAttributeNames() != null) {
                this.updateItemRequest.setExpressionAttributeNames(
                    new HashMap<>(updateItemRequest.getExpressionAttributeNames()));
            }
            if (this.updateItemRequest.getExpressionAttributeValues() != null) {
                this.updateItemRequest.setExpressionAttributeValues(
                    new HashMap<>(updateItemRequest.getExpressionAttributeValues()));
            }
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return updateItemRequest.getExpressionAttributeNames();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (updateItemRequest.getExpressionAttributeNames() == null) {
                updateItemRequest.setExpressionAttributeNames(new HashMap<>());
            }
            updateItemRequest.getExpressionAttributeNames().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            if (updateItemRequest.getExpressionAttributeValues() == null) {
                updateItemRequest.setExpressionAttributeValues(new HashMap<>());
            }
            return updateItemRequest.getExpressionAttributeValues();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            updateItemRequest.getExpressionAttributeValues().put(key, value);
        }

        @Override
        public String getPrimaryExpression() {
            return updateItemRequest.getUpdateExpression();
        }

        @Override
        public void setPrimaryExpression(String expression) {
            updateItemRequest.setUpdateExpression(expression);
        }

        @Override
        public String getFilterExpression() {
            return updateItemRequest.getConditionExpression();
        }

        @Override
        public void setFilterExpression(String conditionalExpression) {
            updateItemRequest.setConditionExpression(conditionalExpression);
        }

        @Override
        public String getIndexName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIndexName(String indexName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Condition> getLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            throw new UnsupportedOperationException();
        }

    }

    private static class DeleteItemRequestWrapper implements RequestWrapper {

        private final DeleteItemRequest deleteItemRequest;

        DeleteItemRequestWrapper(DeleteItemRequest deleteItemRequest) {
            this.deleteItemRequest = deleteItemRequest;
            if (this.deleteItemRequest.getExpressionAttributeNames() != null) {
                this.deleteItemRequest.setExpressionAttributeNames(new HashMap<>(this.getExpressionAttributeNames()));
            }
            if (this.deleteItemRequest.getExpressionAttributeValues() != null) {
                this.deleteItemRequest.setExpressionAttributeValues(new HashMap<>(this.getExpressionAttributeValues()));
            }
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return deleteItemRequest.getExpressionAttributeNames();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (deleteItemRequest.getExpressionAttributeNames() == null) {
                deleteItemRequest.setExpressionAttributeNames(new HashMap<>());
            }
            deleteItemRequest.getExpressionAttributeNames().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            if (deleteItemRequest.getExpressionAttributeValues() == null) {
                deleteItemRequest.setExpressionAttributeValues(new HashMap<>());
            }
            return deleteItemRequest.getExpressionAttributeValues();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            deleteItemRequest.getExpressionAttributeValues().put(key, value);
        }

        @Override
        public String getPrimaryExpression() {
            return deleteItemRequest.getConditionExpression();
        }

        @Override
        public void setPrimaryExpression(String expression) {
            deleteItemRequest.setConditionExpression(expression);
        }

        @Override
        public String getFilterExpression() {
            return null;
        }

        @Override
        public void setFilterExpression(String conditionalExpression) {
        }

        @Override
        public String getIndexName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIndexName(String indexName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Condition> getLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearLegacyExpression() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            throw new UnsupportedOperationException();
        }

    }

}