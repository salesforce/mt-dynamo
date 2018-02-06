/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Transforms query, scan, get|put|update|deleteItem requests and results to/from their underlying physical table.
 *
 * @author msgroi
 */
class MTAmazonDynamoDBByIndexTransformer {

    private final MTAmazonDynamoDBContextProvider mtContext;
    private final String delimiter;
    private final LocalTableDescription physicalTable;
    private final LocalTableDescription virtualTable;
    private final Cache<String, DynamoSecondaryIndex> physicalToVirtualGSICache;
    private final Cache<String, DynamoSecondaryIndex> physicalToVirtualLSICache;

        MTAmazonDynamoDBByIndexTransformer(LocalTableDescription virtualTable,
                                           LocalTableDescription physicalTable,
                                           MTAmazonDynamoDBContextProvider mtContext,
                                           String delimiter) {
        this.mtContext = mtContext;
        this.delimiter = delimiter;
        this.physicalTable = physicalTable;
        this.virtualTable = virtualTable;
        this.physicalToVirtualGSICache = CacheBuilder.newBuilder().build();
        this.physicalToVirtualLSICache = CacheBuilder.newBuilder().build();
    }

    /*
     * Transforms the query or scan request updating the request's index name, table name, and filter expression.
     * The QueryOrScanRequest passed in is only read.  All writes are done via the QueryOrScanRequestCallback methods.
     *
     * Examples, with placeholders
     *
     * Input, with placeholders ...
     *
     * conditionExpression: "#name = :value"
     * expressionAttrNames: { "#name" = "myhkfield" }
     * expressionAttrValues: { ":value" = "somevalue" }
     *
     * Input, without placeholders ...
     *
     * conditionExpression: "myhkfield = :value"
     * expressionAttrValues: { ":value" = "somevalue" }
     *
     * Output ...
     *
     * conditionExpression: "#name = :value"
     * expressionAttrNames: { "#name" = "hk" }
     * expressionAttrValues: { ":value" = "[context]somevalue" }
     *
     * For tables with range keys, does the same, except in step #4, it doesn't apply a prefix
     */
    void transformQueryOrScanRequest(QueryOrScanRequest queryOrScanRequest,
                                     QueryOrScanRequestCallback queryOrScanRequestCallback) {
        boolean usingGSI = false;

        // transform gsi index fields
        if (queryOrScanRequest.getIndexName() != null) {
            Optional<DynamoSecondaryIndex> virtualGSILookup = virtualTable.getGSI(queryOrScanRequest.getIndexName());
            if (virtualGSILookup.isPresent()) {
                usingGSI = true;
                DynamoSecondaryIndex virtualGSI = virtualGSILookup.get();
                DynamoSecondaryIndex physicalGSI = lookupPhysicalGSI(virtualGSILookup.get());
                queryOrScanRequestCallback.setIndexName(physicalGSI.getIndexName());
                try {
                    transformFilterExpression(queryOrScanRequest,
                            queryOrScanRequestCallback,
                            virtualGSI.getHashKey(),
                            physicalGSI.getHashKey(),
                            true);
                } catch (KeyNotPresentException e) {
                    throw new RuntimeException(e.getMessage() + " for table=" + physicalTable.getTableName());
                }
                if (virtualGSI.getRangeKey().isPresent()) {
                    try {
                        transformFilterExpression(queryOrScanRequest,
                                queryOrScanRequestCallback,
                                virtualGSI.getRangeKey().get(),
                                physicalGSI.getRangeKey().get(),
                                false);
                    } catch (KeyNotPresentException e) {
                        throw new RuntimeException(e.getMessage() + " for table=" + physicalTable.getTableName());
                    }
                }
            } else {
                // transform lsi index fields
                Optional<DynamoSecondaryIndex> virtualLSILookup = virtualTable.getLSI(queryOrScanRequest.getIndexName());
                virtualLSILookup.ifPresent(dynamoSecondaryIndex -> {
                    DynamoSecondaryIndex virtualLSI = dynamoSecondaryIndex;
                    DynamoSecondaryIndex physicalLSI = lookupPhysicalLSI(virtualLSI);
                    queryOrScanRequestCallback.setIndexName(physicalLSI.getIndexName());
                    try {
                        transformFilterExpression(queryOrScanRequest,
                                queryOrScanRequestCallback,
                                virtualLSI.getRangeKey().get(),
                                physicalLSI.getRangeKey().get(),
                                false);
                    } catch (KeyNotPresentException e) {
                        throw new RuntimeException(e.getMessage() + " for table=" + physicalTable.getTableName());
                    }
                });
            }
        }

        if (!usingGSI) {
            // transform HASH key
            try {
                transformFilterExpression(queryOrScanRequest,
                        queryOrScanRequestCallback,
                        virtualTable.getHashKey(),
                        physicalTable.getHashKey(),
                        true);
            } catch (KeyNotPresentException e) {
                addBeginsWith(queryOrScanRequest, queryOrScanRequestCallback);
            }

            // transform RANGE key
            if (virtualTable.getRangeKey().isPresent()) {
                try {
                    transformFilterExpression(queryOrScanRequest,
                            queryOrScanRequestCallback,
                            virtualTable.getRangeKey().get(),
                            physicalTable.getRangeKey().get(), false);
                } catch (KeyNotPresentException e) {
                    // range key not present, this is ok
                }
            }
        }

        // transform table name
        queryOrScanRequestCallback.setTableName(physicalTable.getTableName());
    }

    /*
     * Transforms the getItem, putItem, updateItem, deleteItem request by adding/removing/prefixing attributes and
     * updating the request's table name.  The attributes passed in are only read.  All writes are done via the KeyRequestCallback methods.
     */
    void transformKeyRequest(Map<String, AttributeValue> attributes, KeyRequestCallback keyRequestCallback) {
        // hash key
        transformKeyRequestAttribute(attributes,
                                     keyRequestCallback,
                                     virtualTable.getHashKey(),
                                     physicalTable.getHashKey(),
                                     true,
                                     true);

        // range key
        virtualTable.getRangeKey().ifPresent(virtualRangeKeyAttribute -> transformKeyRequestAttribute(attributes,
                                                                                                      keyRequestCallback,
                                                                                                      virtualRangeKeyAttribute,
                                                                                                      physicalTable.getRangeKey().get(),
                                                                                                      false,
                                                                                                      true));

        // gsi keys
        virtualTable.getGSIs().forEach(virtualGSI ->
                transformKeyRequestAttribute(attributes,
                                             keyRequestCallback,
                                             virtualGSI.getHashKey(),
                                             lookupPhysicalGSI(virtualGSI).getHashKey(),
                                             true,
                                             false));

        // lsi keys
        virtualTable.getLSIs().forEach(virtualLSI ->
                transformKeyRequestAttribute(attributes,
                                             keyRequestCallback,
                                             virtualLSI.getRangeKey().get(),
                                             lookupPhysicalLSI(virtualLSI).getRangeKey().get(),
                                             false,
                                             false));

        // table name
        keyRequestCallback.setTableName(physicalTable.getTableName());
    }

    /*
     * Transforms the getItem, query, scan result by adding/removing/de-prefixing attributes.
     *
     * The attributes passed in are only read.  All writes are done via the ResultCallback methods.
     */
    void transformResult(Map<String, AttributeValue> attributes, ResultCallback resultCallback) {
        // hash key
        transformResultAttribute(attributes,
                                 resultCallback,
                                 virtualTable.getHashKey(),
                                 physicalTable.getHashKey(),
                                 true,
                                 true);

        // range key
        if (virtualTable.getRangeKey().isPresent()) {
            transformResultAttribute(attributes,
                                     resultCallback,
                                     virtualTable.getRangeKey().get(),
                                     physicalTable.getRangeKey().get(),
                                     false,
                                     true);
        }

        // gsi attributes
        virtualTable.getGSIs().forEach(virtualGSI ->
                transformResultAttribute(attributes,
                                         resultCallback,
                                         virtualGSI.getHashKey(),
                                         lookupPhysicalGSI(virtualGSI).getHashKey(),
                                         true,
                                         false));

        // lsi attributes
        virtualTable.getLSIs().forEach(virtualLSI ->
                transformResultAttribute(attributes,
                                         resultCallback,
                                         virtualLSI.getRangeKey().get(),
                                         lookupPhysicalLSI(virtualLSI).getRangeKey().get(),
                                         false,
                                         false));
    }

    /*
     * Finds the GSI description in the physical table that matches the key schema types of the virtual index.
     */
    private DynamoSecondaryIndex lookupPhysicalGSI(DynamoSecondaryIndex virtualGSI) {
        try {
            return physicalToVirtualGSICache.get(virtualGSI.getIndexName(), () -> findMatchingSecondaryIndex(physicalTable.getGSIs(), virtualGSI));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Finds the LSI description in the physical table that matches the key schema types of the virtual index.
     */
    private DynamoSecondaryIndex lookupPhysicalLSI(DynamoSecondaryIndex virtualLSI) {
        try {
            return physicalToVirtualLSICache.get(virtualLSI.getIndexName(), () -> findMatchingSecondaryIndex(physicalTable.getLSIs(), virtualLSI));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Finds the physical index matching the virtual provided index by matching HASH and RANGE keys by data type.
     */
    private DynamoSecondaryIndex findMatchingSecondaryIndex(List<DynamoSecondaryIndex> physicalIndexes, DynamoSecondaryIndex virtualIndexToFind) {
        return physicalIndexes.stream().filter(index ->
                // hash key types must match
                index.hashKeyType.equals(virtualIndexToFind.getHashKeyType()) &&
                        // either range key doesn't exist on both
                        ((!virtualIndexToFind.getRangeKey().isPresent() && !index.getRangeKey().isPresent())
                                // or they are present on both and equal
                                || (virtualIndexToFind.getRangeKeyType().get().equals(index.getRangeKeyType().get())))
        ).findFirst().orElseThrow(() -> new RuntimeException("no GSI found in list of available indexes " + physicalIndexes +
                " containing keySchemaElement types of " + virtualIndexToFind));
    }

    private void transformKeyRequestAttribute(Map<String, AttributeValue> attributes,
                                              KeyRequestCallback keyRequestCallback,
                                              String virtualAttrName,
                                              String physicalAttrName,
                                              boolean isHashKey,
                                              boolean failIfAttributeNotFound) {
        if (attributes.get(virtualAttrName) == null) {
            if (failIfAttributeNotFound) {
                throw new RuntimeException("attribute=" + virtualAttrName + " not found in request");
            }
        } else {
            String virtualValue = attributes.get(virtualAttrName).getS();
            String physicalValue = (isHashKey ? buildPhysicalValuePrefix() : "") + virtualValue;
            keyRequestCallback.setAttribute(physicalAttrName, new AttributeValue(physicalValue));
            keyRequestCallback.removeAttribute(virtualAttrName);
        }
    }

    private void transformResultAttribute(Map<String, AttributeValue> attributes,
                                          ResultCallback resultCallback,
                                          String virtualHashKeyAttrName,
                                          String physicalHashKeyAttrName,
                                          boolean isHashKey,
                                          boolean failIfAttributeNotFound) {
        if (attributes.get(physicalHashKeyAttrName) == null) {
            if (failIfAttributeNotFound) {
                throw new RuntimeException("attribute=" + physicalHashKeyAttrName + " not found in request");
            }
        } else {
            String physicalValue = attributes.get(physicalHashKeyAttrName).getS();
            String virtualValue = physicalValue;
            if (isHashKey) {
                String expectedPhysicalValuePrefix = getExpectedPhysicalValuePrefix(physicalValue, physicalHashKeyAttrName);
                virtualValue = physicalValue.substring(expectedPhysicalValuePrefix.length());
            }
            resultCallback.setAttribute(virtualHashKeyAttrName, new AttributeValue(virtualValue));
            resultCallback.removeAttribute(physicalHashKeyAttrName);
        }
    }

    private void transformFilterExpression(QueryOrScanRequest queryOrScanRequest,
                                           QueryOrScanRequestCallback queryOrScanRequestCallback,
                                           String virtualAttrName,
                                           String physicalAttrName,
                                           boolean isHashKey) throws KeyNotPresentException {
        String conditionExpression = queryOrScanRequest.getConditionExpression();
        Map<String, String> expressionAttrNames = queryOrScanRequest.getExpressionAttrNames();
        Map<String, AttributeValue> expressionAttrValues= queryOrScanRequest.getExpressionAttrValues();
        if (conditionExpression == null) {
            // no filter criteria
            throw new KeyNotPresentException("no condition expression specified");
        }

        Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(virtualAttrName)).map(Map.Entry::getKey).findFirst() : Optional.empty();
        String fieldToFind = (keyFieldName.orElse(virtualAttrName));
        String toFind = fieldToFind + " = ";
        int start = conditionExpression.indexOf(toFind);
        if (start == -1) {
            // key not present in filter criteria
            throw new KeyNotPresentException(fieldToFind + " not found in " + conditionExpression);
        } else {
            // key is present in filter criteria
            int end = conditionExpression.indexOf(" ", start + toFind.length());
            String virtualValuePlaceholder = conditionExpression.substring(start + toFind.length(), end == -1 ? conditionExpression.length() : end);
            String virtualValue = expressionAttrValues.get(virtualValuePlaceholder).getS();
            String physicalValue = (isHashKey ? buildPhysicalValuePrefix() : "") + virtualValue;
            expressionAttrValues.put(virtualValuePlaceholder, new AttributeValue(physicalValue));
            if (keyFieldName.isPresent()) {
                expressionAttrNames.put(keyFieldName.get(), physicalAttrName);
            } else {
                queryOrScanRequestCallback.setFilterExpression(conditionExpression.replace(toFind, physicalAttrName + " = "));
            }
        }
    }

    private class KeyNotPresentException extends Exception {
        KeyNotPresentException(String message) {
            super(message);
        }
    }

    private void addBeginsWith(QueryOrScanRequest queryOrScanRequest, QueryOrScanRequestCallback request) {
        String namePlaceholder = "#___name___";
        String physicalValuePrefix = buildPhysicalValuePrefix();
        String valuePlaceholder = ":___value___";
        queryOrScanRequest.getExpressionAttrNames().put(namePlaceholder, physicalTable.getHashKey());
        queryOrScanRequest.getExpressionAttrValues().put(valuePlaceholder, new AttributeValue().withS(physicalValuePrefix));
        request.setFilterExpression((queryOrScanRequest.getConditionExpression() != null ? queryOrScanRequest.getConditionExpression() + " and " : "") +
                                    "begins_with(" + namePlaceholder + ", " + valuePlaceholder + ")");
    }

    private String getExpectedPhysicalValuePrefix(String physicalValue, String physicalHashKeyAttrName) {
        String expectedPhysicalValuePrefix = buildPhysicalValuePrefix();
        checkArgument(physicalValue.startsWith(expectedPhysicalValuePrefix),
                "value of " + physicalHashKeyAttrName + " expected to start with '" +
                        expectedPhysicalValuePrefix + "' but was actually '" + physicalValue + "'");
        return expectedPhysicalValuePrefix;
    }

    private String buildPhysicalValuePrefix() {
        return mtContext.getContext() + delimiter + virtualTable.getTableName()  + delimiter;
    }

    interface KeyRequestCallback {
        void setTableName(String tableName);
        void setAttribute(String attributeName, AttributeValue attributeValue);
        void removeAttribute(String attributeName);
    }

    interface QueryOrScanRequestCallback {
        void setTableName(String tableName);
        void setFilterExpression(String filterExpression);
        void setIndexName(String indexName);
    }

    interface ResultCallback {
        void setAttribute(String attributeName, AttributeValue attributeValue);
        void removeAttribute(String attributeName);
    }

    static class QueryOrScanRequest {
        final String conditionExpression;
        final Map<String, String> expressionAttrNames;
        final Map<String, AttributeValue> expressionAttrValues;
        final String indexName;

        QueryOrScanRequest(String conditionExpression,
                           Map<String, String> expressionAttrNames,
                           Map<String, AttributeValue> expressionAttrValues,
                           String indexName) {
            this.conditionExpression = conditionExpression;
            this.expressionAttrNames = expressionAttrNames;
            this.expressionAttrValues = expressionAttrValues;
            this.indexName = indexName;
        }

        String getConditionExpression() {
            return conditionExpression;
        }
        Map<String, String> getExpressionAttrNames() {
            return expressionAttrNames;
        }
        Map<String, AttributeValue> getExpressionAttrValues() {
            return expressionAttrValues;
        }
        String getIndexName() {
            return indexName;
        }
    }

    static class LocalTableDescription {
        private final TableDescription tableDescription;
        private final String hashKey;
        private final Optional<String> rangeKey;
        private final Map<String, DynamoSecondaryIndex> gsiMap;
        private final Map<String, DynamoSecondaryIndex> lsiMap;

        LocalTableDescription(TableDescription tableDescription) {
            this.tableDescription = tableDescription;
            this.hashKey = this.tableDescription.getKeySchema().stream()
                    .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == HASH)
                    .findFirst()
                    .map(KeySchemaElement::getAttributeName)
                    .orElseThrow((Supplier<RuntimeException>) () ->
                            new IllegalStateException("could not find " + HASH + " for table " + tableDescription.getTableName()));
            this.rangeKey = tableDescription.getKeySchema().stream()
                    .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == RANGE)
                    .map(KeySchemaElement::getAttributeName)
                    .findFirst();
            this.gsiMap = tableDescription.getGlobalSecondaryIndexes() == null ? new HashMap<>() :
                    tableDescription.getGlobalSecondaryIndexes().stream().map(gsi ->
                            new DynamoSecondaryIndex(tableDescription, gsi)).collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
            this.lsiMap = tableDescription.getLocalSecondaryIndexes() == null ? new HashMap<>() :
                    tableDescription.getLocalSecondaryIndexes().stream().map(lsi ->
                            new DynamoSecondaryIndex(tableDescription, lsi)).collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        }

        String getTableName() {
            return tableDescription.getTableName();
        }

        String getHashKey() {
            return hashKey;
        }

        Optional<String> getRangeKey() {
            return rangeKey;
        }

        List<DynamoSecondaryIndex> getGSIs() {
            return new ArrayList<>(gsiMap.values());
        }

        Optional<DynamoSecondaryIndex> getGSI(String indexName) {
            return Optional.ofNullable(gsiMap.get(indexName));
        }

        List<DynamoSecondaryIndex> getLSIs() {
            return new ArrayList<>(lsiMap.values());
        }

        Optional<DynamoSecondaryIndex> getLSI(String indexName) {
            return Optional.ofNullable(lsiMap.get(indexName));
        }
    }

    static class DynamoSecondaryIndex {
        private final String indexName;
        private String hashKey;
        private ScalarAttributeType hashKeyType;
        private Optional<String> rangeKey = Optional.empty();
        private Optional<ScalarAttributeType> rangeKeyType = Optional.empty();

        DynamoSecondaryIndex(TableDescription tableDescription, GlobalSecondaryIndexDescription gsi) {
            this.indexName = gsi.getIndexName();
            setHashAndRangeKeys(tableDescription, gsi.getKeySchema());
        }

        DynamoSecondaryIndex(TableDescription tableDescription, LocalSecondaryIndexDescription lsi) {
            this.indexName = lsi.getIndexName();
            setHashAndRangeKeys(tableDescription, lsi.getKeySchema());
        }

        String getIndexName() {
            return indexName;
        }

        String getHashKey() {
            return hashKey;
        }

        ScalarAttributeType getHashKeyType() {
            return hashKeyType;
        }

        Optional<String> getRangeKey() {
            return rangeKey;
        }

        Optional<ScalarAttributeType> getRangeKeyType() {
            return rangeKeyType;
        }

        private void setHashAndRangeKeys(TableDescription tableDescription, List<KeySchemaElement> keySchemaElements) {
            keySchemaElements.stream().filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == HASH).forEach(keySchemaElement -> {
                hashKey = keySchemaElement.getAttributeName();
                hashKeyType = tableDescription.getAttributeDefinitions().stream()
                        .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(keySchemaElement.getAttributeName()))
                        .map(attributeDefinition -> ScalarAttributeType.valueOf(attributeDefinition.getAttributeType())).findFirst().get();
            });
            keySchemaElements.stream().filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == RANGE).forEach(keySchemaElement -> {
                rangeKey = Optional.of(keySchemaElement.getAttributeName());
                rangeKeyType = tableDescription.getAttributeDefinitions().stream()
                        .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(keySchemaElement.getAttributeName()))
                        .map(attributeDefinition -> ScalarAttributeType.valueOf(attributeDefinition.getAttributeType())).findFirst();
            });
        }

        @Override
        public String toString() {
            return "DynamoSecondaryIndex{" +
                    "indexName='" + indexName + '\'' +
                    ", hashKey='" + hashKey + '\'' +
                    ", hashKeyType=" + hashKeyType +
                    ", rangeKey=" + rangeKey +
                    ", rangeKeyType=" + rangeKeyType +
                    '}';
        }
    }
    
}