/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.cache.MtCache;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows a developer using the mt-dynamo library to provide a custom mapping between tables that clients interact with
 * and the physical tables where the data for those tables are stored.  It support mapping many virtual tables to a
 * single physical table, mapping field names and types, secondary indexes.  It supports for allowing multi-tenant
 * context to be added to table and index hash key fields.  Throughout this documentation, virtual tables are meant to
 * represent tables as they are understood by the developer using the DynamoDB Java API.  Physical tables represent the
 * tables that store the data in AWS.
 *
 * <p>SharedTableCustomDynamicBuilder provides a series of static methods that providing builders that are
 * preconfigured to support a number of common mappings.  See Javadoc for each provided builder for details.
 *
 * <p>Supported methods: create|describe|delete* Table, get|put|update Item, query**, scan**
 *
 * <p>See deleteTableAsync and truncateOnDeleteTable in the SharedTableCustomDynamicBuilder for details on how to
 * control behavior that is specific to deleteTable. ** Only EQ conditions are supported.
 *
 * <p>Deleting and recreating tables without deleting all table data(see truncateOnDeleteTable) may yield unexpected
 * results.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbBySharedTable extends MtAmazonDynamoDbBase {

    private static final Logger log = LoggerFactory.getLogger(MtAmazonDynamoDbBySharedTable.class);

    private final String name;

    private final MtTableDescriptionRepo mtTableDescriptionRepo;
    private final Cache<String, TableMapping> tableMappingCache;
    private final TableMappingFactory tableMappingFactory;
    private final boolean deleteTableAsync;
    private final boolean truncateOnDeleteTable;

    /**
     * TODO: write Javadoc.
     */
    public MtAmazonDynamoDbBySharedTable(String name,
                                         MtAmazonDynamoDbContextProvider mtContext,
                                         AmazonDynamoDB amazonDynamoDb,
                                         TableMappingFactory tableMappingFactory,
                                         MtTableDescriptionRepo mtTableDescriptionRepo,
                                         boolean deleteTableAsync,
                                         boolean truncateOnDeleteTable) {
        super(mtContext, amazonDynamoDb);
        this.name = name;
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        this.tableMappingCache = new MtCache<>(mtContext);
        this.tableMappingFactory = tableMappingFactory;
        this.deleteTableAsync = deleteTableAsync;
        this.truncateOnDeleteTable = truncateOnDeleteTable;
    }

    /**
     * TODO: write Javadoc.
     */
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        return new CreateTableResult().withTableDescription(mtTableDescriptionRepo.createTable(createTableRequest));
    }

    /**
     * TODO: write Javadoc.
     */
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        // map table name
        deleteItemRequest = deleteItemRequest.clone();
        TableMapping tableMapping = getTableMapping(deleteItemRequest.getTableName());
        deleteItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        deleteItemRequest.setKey(tableMapping.getItemMapper().apply(deleteItemRequest.getKey()));

        // delete
        return getAmazonDynamoDb().deleteItem(deleteItemRequest);
    }

    /**
     * TODO: write Javadoc.
     */
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

    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        return new DescribeTableResult().withTable(
            mtTableDescriptionRepo.getTableDescription(describeTableRequest.getTableName()).withTableStatus("ACTIVE"));
    }

    /**
     * TODO: write Javadoc.
     */
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        // map table name
        getItemRequest = getItemRequest.clone();
        TableMapping tableMapping = getTableMapping(getItemRequest.getTableName());
        getItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        getItemRequest.setKey(tableMapping.getItemMapper().apply(getItemRequest.getKey()));

        // map result
        GetItemResult getItemResult = getAmazonDynamoDb().getItem(getItemRequest);
        if (getItemResult.getItem() != null) {
            getItemResult.withItem(tableMapping.getItemMapper().reverse(getItemResult.getItem()));
        }

        return getItemResult;
    }

    private TableMapping getTableMapping(String virtualTableName) {
        try {
            return tableMappingCache.get(virtualTableName, () ->
                tableMappingFactory.getTableMapping(
                    new DynamoTableDescriptionImpl(mtTableDescriptionRepo.getTableDescription(virtualTableName))));
        } catch (ExecutionException e) {
            throw new RuntimeException("exception mapping virtual table " + virtualTableName, e);
        }
    }

    /**
     * TODO: write Javadoc.
     */
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        // map table name
        putItemRequest = putItemRequest.clone();
        TableMapping tableMapping = getTableMapping(putItemRequest.getTableName());
        putItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map item
        putItemRequest.setItem(tableMapping.getItemMapper().apply(putItemRequest.getItem()));

        // put
        return getAmazonDynamoDb().putItem(putItemRequest);
    }

    /**
     * TODO: write Javadoc.
     */
    public QueryResult query(QueryRequest queryRequest) {
        // map table name
        queryRequest = queryRequest.clone();
        TableMapping tableMapping = getTableMapping(queryRequest.getTableName());
        queryRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map query request
        tableMapping.getQueryMapper().apply(queryRequest);

        // map result
        QueryResult queryResult = getAmazonDynamoDb().query(queryRequest);
        queryResult.setItems(
            queryResult.getItems().stream().map(item -> tableMapping.getItemMapper().reverse(item)).collect(toList()));

        return queryResult;
    }

    /**
     * TODO: write Javadoc.
     */
    public ScanResult scan(ScanRequest scanRequest) {
        TableMapping tableMapping = getTableMapping(scanRequest.getTableName());
        PrimaryKey key = scanRequest.getIndexName() == null ? tableMapping.getVirtualTable().getPrimaryKey()
            : tableMapping.getVirtualTable().findSi(scanRequest.getIndexName()).getPrimaryKey();

        // Projection must include primary key, since we use it for paging.
        // (We could add key fields into projection and filter result in the future)
        Preconditions.checkArgument(projectionContainsKey(scanRequest, key),
            "Multitenant scans must include key in projection expression");

        // map table name
        ScanRequest clonedScanRequest = scanRequest.clone();
        clonedScanRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map query request
        clonedScanRequest.setExpressionAttributeNames(Optional.ofNullable(clonedScanRequest.getFilterExpression())
            .map(s -> new HashMap<>(clonedScanRequest.getExpressionAttributeNames())).orElseGet(HashMap::new));
        clonedScanRequest.setExpressionAttributeValues(Optional.ofNullable(clonedScanRequest.getFilterExpression())
            .map(s -> new HashMap<>(clonedScanRequest.getExpressionAttributeValues())).orElseGet(HashMap::new));
        tableMapping.getQueryMapper().apply(clonedScanRequest);

        // scan until we find at least one record for current tenant or reach end
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

    private boolean projectionContainsKey(ScanRequest request, PrimaryKey key) {
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

    private boolean projectionContainsKey(String projection, Map<String, String> expressionNames,
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
     * TODO: write Javadoc.
     */
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        // map table name
        updateItemRequest = updateItemRequest.clone();
        TableMapping tableMapping = getTableMapping(updateItemRequest.getTableName());
        updateItemRequest.withTableName(tableMapping.getPhysicalTable().getTableName());

        // map key
        updateItemRequest.setKey(tableMapping.getItemMapper().apply(updateItemRequest.getKey()));

        // map attributeUpdates // TODO msgroi todo

        // map updateCondition // TODO msgroi todo

        // update
        return getAmazonDynamoDb().updateItem(updateItemRequest);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public List<MtStreamDescription> listStreams(IRecordProcessorFactory factory) {
        return tableMappingCache.asMap().values().stream()
            .map(TableMapping::getPhysicalTable)
            .filter(physicalTable -> Optional.ofNullable(physicalTable.getStreamSpecification())
                .map(StreamSpecification::isStreamEnabled).orElse(false))
            .map(physicalTable -> new MtStreamDescription()
                .withLabel(physicalTable.getTableName())
                .withArn(physicalTable.getLastStreamArn())
                .withRecordProcessorFactory(newAdapter(factory, physicalTable))).collect(toList());
    }

    private IRecordProcessorFactory newAdapter(IRecordProcessorFactory factory, DynamoTableDescription physicalTable) {
        return () -> new RecordProcessor(factory.createProcessor(), physicalTable);
    }

    private class RecordProcessor implements IRecordProcessor {

        private final IRecordProcessor processor;
        private final DynamoTableDescription physicalTable;

        RecordProcessor(IRecordProcessor processor, DynamoTableDescription physicalTable) {
            this.processor = processor;
            this.physicalTable = physicalTable;
        }

        @Override
        public void initialize(InitializationInput initializationInput) {
            processor.initialize(initializationInput);
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            List<com.amazonaws.services.kinesis.model.Record> records = processRecordsInput.getRecords().stream()
                .map(RecordAdapter.class::cast).map(this::toMtRecord).collect(toList());
            processor.processRecords(processRecordsInput.withRecords(records));
        }

        private com.amazonaws.services.kinesis.model.Record toMtRecord(RecordAdapter adapter) {
            Record r = adapter.getInternalObject();
            StreamRecord streamRecord = r.getDynamodb();
            FieldValue fieldValue = new FieldPrefixFunction(".")
                .reverse(streamRecord.getKeys().get(physicalTable.getPrimaryKey().getHashKey()).getS());
            MtAmazonDynamoDbContextProvider mtContext = getMtContext();
            TableMapping tableMapping;
            try {
                mtContext.setContext(fieldValue.getMtContext());
                tableMapping = getTableMapping(
                    fieldValue.getTableIndex()); // getting a table mapping requires tenant context
            } finally {
                mtContext.setContext(null);
            }
            ItemMapper itemMapper = tableMapping.getItemMapper();
            streamRecord.setKeys(itemMapper.reverse(streamRecord.getKeys()));
            streamRecord.setOldImage(itemMapper.reverse(streamRecord.getOldImage()));
            streamRecord.setNewImage(itemMapper.reverse(streamRecord.getNewImage()));
            return new RecordAdapter(new MtRecord()
                .withAwsRegion(r.getAwsRegion())
                .withDynamodb(streamRecord)
                .withEventID(r.getEventID())
                .withEventName(r.getEventName())
                .withEventSource(r.getEventSource())
                .withEventVersion(r.getEventVersion())
                .withContext(fieldValue.getMtContext())
                .withTableName(fieldValue.getTableIndex()));
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            processor.shutdown(shutdownInput);
        }

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
            log.warn("truncation of " + scanResult.getItems().size() + " items from table=" + tableName + " complete");
        } else {
            log.info("truncateOnDeleteTable is disabled for " + tableName + ", skipping truncation");
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
}