/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
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
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.google.common.base.Predicate;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndexTransformer.KeyRequestCallback;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndexTransformer.QueryOrScanRequest;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndexTransformer.QueryOrScanRequestCallback;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndexTransformer.ResultCallback;

/**
 * Allows for storing all tenant data in shared table, separated by prefixing the table's hash key field with the multi-tenant
 * context.
 *
 * To use, call the static builder() method.  The following arguments are required ...
 * - an AmazonDynamoDB instance
 * - a multi-tenant context provided
 * - an MTTableDescriptionRepo instance for storing and retrieving table descriptions
 *
 * The following are optional arguments ...
 * - delimiter: a String delimiter used to separate the tenant identifier prefix from the hashkey value
 * - truncateOnDeleteTable: a boolean to indicate whether all of a table's data should be deleted when a table is dropped, default: FALSE
 * - deleteTableAsync: a boolean to indicate whether the table data deletion may happen asynchronously after the table is dropped, default: FALSE
 * - tablePrefix: a String used to prefix all tables with, independently of mulit-tenant context, to provide the ability to support multiple environments within an account
 * - defaultCU: a Long used to configure table and index-level read/write capacity units, the more specific below arguments will override the provided defaultCU when they are provided
 * - tableRCU: a Long used to configure table read capacity
 * - tableWCU: a Long used to configure table write capacity
 * - gsiRCU: a Long used to configure index read capacity
 * - gsiWCU: a Long used to configure index write capacity
 *
 * MTDynamoDBTableDescriptionRepo provides a default implementation of MTTableDescriptionRepo.  It requires ...
 * - an AmazonDynamoDB instance(must not, itself, be a multi-tenant AmazonDynamoDB)
 * - a multi-tenant context
 * - polling interval for how long it will wait between requests for the status of table while it's being created
 *
 * Supported:
 *    - methods: create|describe|delete Table, get|putItem, scan*, query**
 *    - tables: only supports table with a primary key's containing only a HASH field of type STRING, or a table containing
 *    a HASH field and a RANGE field both of type STRING
 *
 * * Only using filterExpression' are support since, according to DynamoDB docs, scanFilters are considered a 'legacy parameter'.
 * ** Only using keyConditionExpression's are supported since, according to DynamoDB docs, keyConditions
 *    is considered a 'legacy parameter'.
 * @author msgroi
 */
public class MTAmazonDynamoDBByIndex extends MTAmazonDynamoDBBase {

    private static final Logger log = LoggerFactory.getLogger(MTAmazonDynamoDBByIndex.class);
    private final MTTableDescriptionRepo mtTableDescriptionRepo;
    private final boolean deleteTableAsync;
    private final boolean truncateOnDeleteTable;
    private final MTAmazonDynamoDBByIndexTransformerBuilder indexTransformerCache;

    private MTAmazonDynamoDBByIndex(MTAmazonDynamoDBContextProvider mtContext,
                                    AmazonDynamoDB amazonDynamoDB,
                                    int pollIntervalSeconds,
                                    MTTableDescriptionRepo mtTableDescriptionRepo,
                                    String delimiter,
                                    boolean deleteTableAsync,
                                    boolean truncateOnDeleteTable,
                                    Optional<String> tablePrefix,
                                    long tableRCU,
                                    long tableWCU,
                                    long gsiRCU,
                                    long gsiWCU) {
        super(mtContext, amazonDynamoDB);
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        this.deleteTableAsync = deleteTableAsync;
        this.truncateOnDeleteTable = truncateOnDeleteTable;
        indexTransformerCache = new MTAmazonDynamoDBByIndexTransformerBuilder(amazonDynamoDB,
                                                                              mtContext,
                                                                              delimiter,
                                                                              pollIntervalSeconds,
                                                                              tablePrefix,
                                                                              tableRCU,
                                                                              tableWCU,
                                                                              gsiRCU,
                                                                              gsiWCU);
    }

    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        validateCreateTableRequest(createTableRequest);
        return new CreateTableResult().withTableDescription(mtTableDescriptionRepo.createTable(createTableRequest));
    }

    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        DeleteItemRequest clonedDeleteItemRequest = deleteItemRequest.clone();
        clonedDeleteItemRequest.setKey(new HashMap<>(deleteItemRequest.getKey()));

        getIndexTransformer(deleteItemRequest.getTableName()).transformKeyRequest(deleteItemRequest.getKey(), new KeyRequestCallback() {
            @Override
            public void setTableName(String tableName) {
                clonedDeleteItemRequest.setTableName(tableName);
            }
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                clonedDeleteItemRequest.getKey().put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
                clonedDeleteItemRequest.getKey().remove(attributeName);
            }
        });

        return getAmazonDynamoDB().deleteItem(clonedDeleteItemRequest);
    }

    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        if (deleteTableAsync) {
            Executors.newSingleThreadExecutor().submit(() -> {
                deleteTableInternal(deleteTableRequest);
            });
            return new DeleteTableResult().withTableDescription(mtTableDescriptionRepo.getTableDescription(deleteTableRequest.getTableName()));
        } else {
            return deleteTableInternal(deleteTableRequest);
        }
    }

    private DeleteTableResult deleteTableInternal(DeleteTableRequest deleteTableRequest) {
        String tableDesc = "table=" + deleteTableRequest.getTableName() + " " + (deleteTableAsync ? "asynchronously" : "synchronously");
        log.warn("dropping " + tableDesc);
        truncateTable(deleteTableRequest.getTableName());
        DeleteTableResult deleteTableResult = new DeleteTableResult().withTableDescription(mtTableDescriptionRepo.deleteTable(deleteTableRequest.getTableName()));
        indexTransformerCache.invalidateTransformerCache(deleteTableRequest.getTableName());
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

    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        return new DescribeTableResult().withTable(mtTableDescriptionRepo.getTableDescription(describeTableRequest.getTableName()).withTableStatus("ACTIVE"));
    }

    public GetItemResult getItem(GetItemRequest getItemRequest) {
        // clone request
        GetItemRequest clonedGetItemRequest = getItemRequest.clone();
        clonedGetItemRequest.setKey(new HashMap<>(getItemRequest.getKey()));

        // get index transformer
        MTAmazonDynamoDBByIndexTransformer transformer = getIndexTransformer(getItemRequest.getTableName());

        // transform the key element in the get request
        transformer.transformKeyRequest(getItemRequest.getKey(), new KeyRequestCallback() {
            @Override
            public void setTableName(String tableName) {
                clonedGetItemRequest.setTableName(tableName);
            }
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                clonedGetItemRequest.getKey().put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
                clonedGetItemRequest.getKey().remove(attributeName);
            }
        });

        // perform get
        GetItemResult getItemResult = getAmazonDynamoDB().getItem(clonedGetItemRequest);

        // transform the key attribute in the returned item
        if (getItemResult.getItem() != null) {
            transformer.transformResult(getItemResult.getItem(), new ResultCallback() {
                @Override
                public void setAttribute(String attributeName, AttributeValue attributeValue) {
                    getItemResult.getItem().put(attributeName, attributeValue);
                }
                @Override
                public void removeAttribute(String attributeName) {
                    getItemResult.getItem().remove(attributeName);
                }
            });
        }

        return getItemResult;
    }

    public PutItemResult putItem(PutItemRequest putItemRequest) {
        PutItemRequest clonedPutItemRequest = putItemRequest.clone();
        clonedPutItemRequest.setItem(new HashMap<>(putItemRequest.getItem()));

        getIndexTransformer(putItemRequest.getTableName()).transformKeyRequest(putItemRequest.getItem(), new KeyRequestCallback() {
            @Override
            public void setTableName(String tableName) {
                clonedPutItemRequest.setTableName(tableName);
            }
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                clonedPutItemRequest.getItem().put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
            }
        });

        return getAmazonDynamoDB().putItem(clonedPutItemRequest);
    }

    public QueryResult query(QueryRequest queryRequest) {
        // clone request
        QueryRequest clonedQueryRequest = queryRequest.clone();
        if (queryRequest.getExpressionAttributeNames() != null) {
            clonedQueryRequest.setExpressionAttributeNames(new HashMap<>(queryRequest.getExpressionAttributeNames()));
        }
        clonedQueryRequest.setExpressionAttributeValues(new HashMap<>(queryRequest.getExpressionAttributeValues()));

        // get index transformer
        MTAmazonDynamoDBByIndexTransformer transformer = getIndexTransformer(queryRequest.getTableName());

        // replace hashkey attribute name and value
        transformer.transformQueryOrScanRequest(
                new QueryOrScanRequest(queryRequest.getKeyConditionExpression(),
                                       clonedQueryRequest.getExpressionAttributeNames(),
                                       clonedQueryRequest.getExpressionAttributeValues(),
                                       queryRequest.getIndexName()),
                new QueryOrScanRequestCallback() {
            @Override
            public void setTableName(String tableName) {
                clonedQueryRequest.setTableName(tableName);
            }
            @Override
            public void setFilterExpression(String filterExpression) {
                clonedQueryRequest.setKeyConditionExpression(filterExpression);
            }
            @Override
            public void setIndexName(String indexName) {
                clonedQueryRequest.setIndexName(indexName);
            }
        });

        // issue the request
        QueryResult queryResult = getAmazonDynamoDB().query(clonedQueryRequest);

        // transform the key attribute in each returned item
        queryResult.getItems().forEach(item -> transformer.transformResult(item, new ResultCallback() {
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                item.put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
                item.remove(attributeName);
            }
        }));

        return queryResult;
    }

    public ScanResult scan(ScanRequest scanRequest) {
        // get filter expression
        Optional<String> filterExpression = Optional.ofNullable(scanRequest.getFilterExpression());

        // clone request
        ScanRequest clonedScanRequest = scanRequest.clone();
        clonedScanRequest.setExpressionAttributeNames(filterExpression.map(s -> new HashMap<>(scanRequest.getExpressionAttributeNames())).orElseGet(HashMap::new));
        clonedScanRequest.setExpressionAttributeValues(filterExpression.map(s -> new HashMap<>(scanRequest.getExpressionAttributeValues())).orElseGet(HashMap::new));

        // get index transformer
        MTAmazonDynamoDBByIndexTransformer transformer = getIndexTransformer(scanRequest.getTableName());

        // create request to be used as input to transformer
        QueryOrScanRequest queryOrScanRequest = new QueryOrScanRequest(scanRequest.getFilterExpression(),
                clonedScanRequest.getExpressionAttributeNames(),
                clonedScanRequest.getExpressionAttributeValues(),
                clonedScanRequest.getIndexName());

        if (scanRequest.getFilterExpression() != null) {
            // replace hashkey attribute name and value
            transformer.transformQueryOrScanRequest(
                       queryOrScanRequest,
                       new QueryOrScanRequestCallback() {
                            @Override
                            public void setTableName(String tableName) {
                                clonedScanRequest.setTableName(tableName);
                            }
                            @Override
                            public void setFilterExpression(String filterExpression) {
                                clonedScanRequest.setFilterExpression(filterExpression);
                            }
                            @Override
                            public void setIndexName(String indexName) {
                                clonedScanRequest.setIndexName(indexName);
                            }
                    });
        } else {
            // scan all
            transformer.transformQueryOrScanRequest(
                       queryOrScanRequest,
                       new QueryOrScanRequestCallback() {
                            @Override
                            public void setTableName(String tableName) {
                                clonedScanRequest.setTableName(tableName);
                            }
                            @Override
                            public void setFilterExpression(String filterExpression) {
                                clonedScanRequest.setFilterExpression(filterExpression);
                            }
                            @Override
                            public void setIndexName(String indexName) {
                                clonedScanRequest.setIndexName(indexName);
                            }
                    });
        }

        // issue the request
        ScanResult scanResult = getAmazonDynamoDB().scan(clonedScanRequest);

        // transform the key attribute in each returned item
        scanResult.getItems().forEach(item -> transformer.transformResult(item, new ResultCallback() {
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                item.put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
                item.remove(attributeName);
            }
        }));

        return scanResult;
    }

    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        UpdateItemRequest clonedUpdateItemRequest = updateItemRequest.clone();
        clonedUpdateItemRequest.setKey(new HashMap<>(updateItemRequest.getKey()));

        getIndexTransformer(updateItemRequest.getTableName()).transformKeyRequest(updateItemRequest.getKey(), new KeyRequestCallback() {
            @Override
            public void setTableName(String tableName) {
                clonedUpdateItemRequest.setTableName(tableName);
            }
            @Override
            public void setAttribute(String attributeName, AttributeValue attributeValue) {
                clonedUpdateItemRequest.getKey().put(attributeName, attributeValue);
            }
            @Override
            public void removeAttribute(String attributeName) {
                clonedUpdateItemRequest.getKey().remove(attributeName);
            }
        });
        return getAmazonDynamoDB().updateItem(clonedUpdateItemRequest);
    }

    private MTAmazonDynamoDBByIndexTransformer getIndexTransformer(String virtualTableName) {
        return indexTransformerCache.getIndexTransformer(describeTable(virtualTableName).getTable());
    }

    @Override
    public List<MTStreamDescription> listStreams(IRecordProcessorFactory factory) {
        throw new UnsupportedOperationException();
    }

    public static MTAmazonDynamoDBByIndexBuilder builder() {
        return new MTAmazonDynamoDBByIndexBuilder();
    }

    public static class MTAmazonDynamoDBByIndexBuilder {

        private AmazonDynamoDB amazonDynamoDB;
        private MTAmazonDynamoDBContextProvider mtContext;
        private MTTableDescriptionRepo mtTableDescriptionRepo;
        private Integer pollIntervalSeconds;
        private String delimiter;
        private Boolean deleteTableAsync;
        private Boolean truncateOnDeleteTable;
        private Optional<String> tablePrefix;
        private Long defaultCU;
        private Long tableRCU;
        private Long tableWCU;
        private Long gsiRCU;
        private Long gsiWCU;

        public MTAmazonDynamoDBByIndexBuilder withContext(MTAmazonDynamoDBContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withAmazonDynamoDB(AmazonDynamoDB amazonDynamoDB) {
            this.amazonDynamoDB = amazonDynamoDB;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withPollIntervalSeconds(Integer pollIntervalSeconds) {
            this.pollIntervalSeconds = pollIntervalSeconds;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withTableDescriptionRepo(MTTableDescriptionRepo mtTableDescriptionRepo) {
            this.mtTableDescriptionRepo = mtTableDescriptionRepo;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withDeleteTableAsync(boolean dropAsync) {
            this.deleteTableAsync = dropAsync;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withTruncateOnDeleteTable(Boolean truncateOnDrop) {
            this.truncateOnDeleteTable = truncateOnDrop;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withTablePrefix(String tablePrefix) {
            this.tablePrefix = Optional.of(tablePrefix);
            return this;
        }

        /*
         * defaultCU gets applied to any unspecified table or GSI RCU/WCU setting.
         */
        public MTAmazonDynamoDBByIndexBuilder withDefaultCU(long defaultCU) {
            this.defaultCU = defaultCU;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withTableRCU(long tableRCU) {
            this.tableRCU = tableRCU;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withTableWCU(long tableWCU) {
            this.tableWCU = tableWCU;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withGSIRCU(long gsiRCU) {
            this.gsiRCU = gsiRCU;
            return this;
        }

        public MTAmazonDynamoDBByIndexBuilder withGSIWCU(long gsiWCU) {
            this.gsiWCU = gsiWCU;
            return this;
        }

        public MTAmazonDynamoDBByIndex build() {
            setDefaults();
            checkArgument(mtContext != null,"mtContext is required");
            checkArgument(amazonDynamoDB != null,"amazonDynamoDB is required");
            checkArgument(mtTableDescriptionRepo != null,"mtTableDescriptionRepo is required");
            return new MTAmazonDynamoDBByIndex(mtContext,
                                               amazonDynamoDB,
                                               pollIntervalSeconds,
                                               mtTableDescriptionRepo,
                                               delimiter,
                                               deleteTableAsync,
                                               truncateOnDeleteTable,
                                               tablePrefix,
                                               tableRCU,
                                               tableWCU,
                                               gsiRCU,
                                               gsiWCU);
        }

        private void setDefaults() {
            if (pollIntervalSeconds == null) {
                pollIntervalSeconds = 1;
            }
            if (delimiter == null) {
                delimiter = ".";
            }
            if (truncateOnDeleteTable == null) {
                truncateOnDeleteTable = false;
            }
            if (deleteTableAsync == null) {
                deleteTableAsync = false;
            }
            if (tablePrefix == null) {
                tablePrefix = Optional.empty();
            }
            if (defaultCU == null) {
                defaultCU = 1L;
            }
            if (tableRCU == null) {
                tableRCU = defaultCU;
            }
            if (tableWCU == null) {
                tableWCU = defaultCU;
            }
            if (gsiRCU == null) {
                gsiRCU = defaultCU;
            }
            if (gsiWCU == null) {
                gsiWCU = defaultCU;
            }
        }

    }

    private void validateCreateTableRequest(CreateTableRequest createTableRequest) {
        checkNotNull( createTableRequest.getKeySchema(), "keySchema is required");
        Optional<KeySchemaElement> hashKeySchemaElement = createTableRequest.getKeySchema().stream()
                .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == HASH).findAny();
        Optional<KeySchemaElement> rangeKeySchemaElement = createTableRequest.getKeySchema().stream()
                .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == RANGE).findAny();
        checkNotNull(hashKeySchemaElement, "keyElement of type HASH is required, table=" + createTableRequest.getTableName());
        Optional<AttributeDefinition> hashKeyFieldAttributeDefinition = createTableRequest.getAttributeDefinitions().stream()
                .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(hashKeySchemaElement.get().getAttributeName())).findAny();
        checkNotNull("attributeDefinition for keyElement field '" + hashKeySchemaElement.get().getAttributeName() + " is required, table=" + createTableRequest.getTableName());
        checkArgument(ScalarAttributeType.valueOf(hashKeyFieldAttributeDefinition.get().getAttributeType()) == S, "keyElement HASH field must be of type 'S', table=" + createTableRequest.getTableName());
        rangeKeySchemaElement.ifPresent(keySchemaElement -> {
            Optional<AttributeDefinition> rangeKeyFieldAttributeDefinition = createTableRequest.getAttributeDefinitions().stream()
                    .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(keySchemaElement.getAttributeName())).findAny();
            checkNotNull("attributeDefinition for keyElement field '" + keySchemaElement.getAttributeName() + " is required, table=" + createTableRequest.getTableName());
            checkArgument(ScalarAttributeType.valueOf(rangeKeyFieldAttributeDefinition.get().getAttributeType()) == S, "keyElement RANGE field must be of type 'S', table=" + createTableRequest.getTableName());
        });
        if (createTableRequest.getGlobalSecondaryIndexes() != null && createTableRequest.getGlobalSecondaryIndexes().size() > 0) {
            String msg = "only a single GSI with a single element HASH key of type 'S' is currently supported";
            checkArgument(createTableRequest.getGlobalSecondaryIndexes().get(0).getKeySchema().size() == 1, msg);
            KeySchemaElement gsiHashKeySchemaElement = createTableRequest.getGlobalSecondaryIndexes().get(0).getKeySchema().get(0);

            /*
             * The following code resulted in the following stack when used as a dependent jar ...
             *
             * Optional<AttributeDefinition> gsiHashKeyAttrDef = createTableRequest.getAttributeDefinitions().stream().filter((Predicate<AttributeDefinition>) attrDef ->
             *                                                          attrDef.getAttributeName().equalsIgnoreCase(gsiHashKeySchemaElement.getAttributeName())).findFirst();
             *
             * java.lang.IncompatibleClassChangeError: null
             * at java.util.stream.ReferencePipeline$2$1.accept(ReferencePipeline.java:174)
             * at java.util.ArrayList$ArrayListSpliterator.tryAdvance(ArrayList.java:1351)
             * at java.util.stream.ReferencePipeline.forEachWithCancel(ReferencePipeline.java:126)
             * at java.util.stream.AbstractPipeline.copyIntoWithCancel(AbstractPipeline.java:498)
             * at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:485)
             * at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471)
             * at java.util.stream.FindOps$FindOp.evaluateSequential(FindOps.java:152)
             * at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
             * at java.util.stream.ReferencePipeline.findFirst(ReferencePipeline.java:464)
             * at com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndex.validateCreateTableRequest(MTAmazonDynamoDBByIndex.java:499)
             */
            Optional<AttributeDefinition> gsiHashKeyAttrDef = Optional.empty();
            for (AttributeDefinition attrDef: createTableRequest.getAttributeDefinitions()) {
                if (attrDef.getAttributeName().equalsIgnoreCase(gsiHashKeySchemaElement.getAttributeName())) {
                    gsiHashKeyAttrDef = Optional.of(attrDef);
                    break;
                }
            }
            checkArgument(gsiHashKeyAttrDef.isPresent(), msg);
            checkArgument(KeyType.valueOf(gsiHashKeySchemaElement.getKeyType()) == HASH &&
                            ScalarAttributeType.valueOf(gsiHashKeyAttrDef.get().getAttributeType()) == S,
                    msg);
        }
        if (createTableRequest.getLocalSecondaryIndexes() != null && createTableRequest.getLocalSecondaryIndexes().size() > 0) {
            checkArgument(createTableRequest.getLocalSecondaryIndexes().get(0).getKeySchema().size() == 2 &&
                            KeyType.valueOf(createTableRequest.getLocalSecondaryIndexes().get(0).getKeySchema().get(0).getKeyType()) == HASH &&
                            KeyType.valueOf(createTableRequest.getLocalSecondaryIndexes().get(0).getKeySchema().get(1).getKeyType()) == RANGE &&
                            ScalarAttributeType.valueOf(createTableRequest.getAttributeDefinitions().stream().filter((Predicate<AttributeDefinition>) attrDef ->
                                    attrDef.getAttributeName().equals(createTableRequest.getLocalSecondaryIndexes().get(0).getKeySchema().get(0).getAttributeName()) ||
                                    attrDef.getAttributeName().equals(createTableRequest.getLocalSecondaryIndexes().get(0).getKeySchema().get(1).getAttributeName())).findFirst().get().getAttributeType()) == S,
                    "only a single LSI with a HASH key of type 'S' and RANGE key of type 'S' is currently supported");
        }
        checkArgument(createTableRequest.getStreamSpecification() == null, "streamSpecified may not be specified, table=" + createTableRequest.getTableName());
    }

    public interface MTTableDescriptionRepo {
        TableDescription createTable(CreateTableRequest createTableRequest);
        TableDescription getTableDescription(String tableName);
        TableDescription deleteTable(String tableName);
    }

}