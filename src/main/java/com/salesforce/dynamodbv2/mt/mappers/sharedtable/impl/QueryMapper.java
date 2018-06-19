/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.isEmpty;

/*
 * Maps query and scan requests against virtual tables to their physical table counterpart according to the provided
 * TableMapping, delegating field mapping to the provided FieldMapper.
 *
 * @author msgroi
 */
class QueryMapper {

    private final FieldMapper fieldMapper;
    private final TableMapping tableMapping;

    QueryMapper(TableMapping tableMapping, FieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.tableMapping = tableMapping;
    }

    /*
     * Takes a QueryRequest representing a query against a virtual table and mutates it so it can be applied to its physical table counterpart.
     */
    void apply(QueryRequest queryRequest) {
        validateQueryRequest(queryRequest);
        convertLegacyExpression(new QueryRequestWrapper(queryRequest));
        applyKeyCondition(new QueryRequestWrapper(queryRequest));
    }

    /*
     * Takes a ScanRequest representing a scan against a virtual table and mutates it so it can be applied to its physical table counterpart.
     */
    void apply(ScanRequest scanRequest) {
        validateScanRequest(scanRequest);
        convertLegacyExpression(new ScanRequestWrapper(scanRequest));
        applyKeyCondition(new ScanRequestWrapper(scanRequest));
    }

    private void applyKeyCondition(RequestWrapper request) {
        String virtualHashKey;
        Collection<FieldMapping> fieldMappings;

        if (request.getIndexName() == null) {
            // query or scan does NOT use index
            virtualHashKey = tableMapping.getVirtualTable().getPrimaryKey().getHashKey();
            Map<String, List<FieldMapping>> fieldMappingUnduped = tableMapping.getAllVirtualToPhysicalFieldMappings();
            fieldMappings = dedupFieldMappings(fieldMappingUnduped).values();
        } else {
            // query uses index
            DynamoSecondaryIndex virtualSecondaryIndex = tableMapping.getVirtualTable().findSI(request.getIndexName());
            fieldMappings = tableMapping.getIndexPrimaryKeyFieldMappings(virtualSecondaryIndex);
            request.setIndexName(((List<FieldMapping>) fieldMappings).get(0).getPhysicalIndexName());
            virtualHashKey = virtualSecondaryIndex.getPrimaryKey().getHashKey();
        }

        convertFieldNameLiteralsToExpressionNames(fieldMappings, request);

        if (!queryContainsHashKeyCondition(request, virtualHashKey)) {
            // the expression does not contain the table or index key that's being used in the query, add begins_with clause
            String physicalHashKey = fieldMappings.stream().filter((Predicate<FieldMapping>) fieldMapping ->
                    fieldMapping.getSource().getName().equals(virtualHashKey)).findFirst()
                    .orElseThrow((Supplier<IllegalArgumentException>) () ->
                    new IllegalArgumentException("field mapping not found hashkey field " + virtualHashKey)).getTarget().getName();
            FieldMapping fieldMapping = fieldMappings.stream().filter((Predicate<FieldMapping>) fieldMapping1 ->
                    fieldMapping1.getSource().getName().equals(virtualHashKey)).findFirst()
                    .orElseThrow((Supplier<IllegalArgumentException>) () ->
                            new IllegalArgumentException("field mapping not found hashkey field " + virtualHashKey));
            addBeginsWith(request, physicalHashKey, fieldMapping);
        }

        checkNotNull(request.getExpression(), "request expression is required");

        // map each field to its target name and apply field prefixing as appropriate
        fieldMappings.forEach(targetFieldMapping -> applyKeyConditionToField(request, targetFieldMapping));
    }

    @VisibleForTesting
    void convertFieldNameLiteralsToExpressionNames(Collection<FieldMapping> fieldMappings, // TODO msgroi unit test
                                                   RequestWrapper request) {
        AtomicInteger counter = new AtomicInteger(1);
        fieldMappings.forEach(fieldMapping -> {
            String virtualFieldName = fieldMapping.getSource().getName();
            String toFind = " " + virtualFieldName + " =";
            int start = (" " + request.getExpression()).indexOf(toFind); // TODO msgroi look for other operators, deal with space
            while (start >= 0) {
                String fieldLiteral = request.getExpression().substring(start, start + virtualFieldName.length());
                String fieldPlaceholder = getNextFieldPlaceholder(request.getExpressionAttributeNames(), counter);
                request.setExpression(request.getExpression().replaceAll(fieldLiteral + " ", fieldPlaceholder + " "));
                request.putExpressionAttributeName(fieldPlaceholder, fieldLiteral);
                start = (" " + request.getExpression()).indexOf(toFind);
            }
        });
    }

    private String getNextFieldPlaceholder(Map<String, String> expressionAttributeNames, AtomicInteger counter) {
        String fieldPlaceholderCandidate = "#field" + counter.get();
        while (expressionAttributeNames != null && expressionAttributeNames.containsKey(fieldPlaceholderCandidate)) {
            fieldPlaceholderCandidate = "#field" + counter.incrementAndGet();
        }
        return fieldPlaceholderCandidate;
    }

    /*
     * This method takes a mapping of virtual to physical fields, where it is possible that a single given virtual
     * field may map to more than one physical field, and returns a mapping where each virtual field maps to exactly
     * one physical field.  In cases where there is more than one physical field for a given virtual field, it
     * arbitrarily chooses the first mapping.
     *
     * This method is called for any query or scan request that does not specify an index.
     *
     * It is an effective no-op, meaning, there are no duplicates to remove, except when a scan is performed against a table
     * that maps a given virtual field to multiple physical fields.  In that case, it doesn't matter which field we use
     * in the query, the results should be the same, so we choose one of the physical fields arbitrarily.
     */
    private Map<String, FieldMapping> dedupFieldMappings(Map<String, List<FieldMapping>> fieldMappings) {
        return fieldMappings.entrySet().stream().collect(Collectors.toMap(
                Entry::getKey,
                fieldMappingEntry -> fieldMappingEntry.getValue().get(0)
                ));
    }
    private void addBeginsWith(RequestWrapper request, String hashKey, FieldMapping fieldMapping) {
        String namePlaceholder = "#___name___";
        // TODO make sure it properly identifies that it doesn't need to add this ... make sure it's an equals condition and that the equals condition can't be hacked ... make sure you can't negate the begins_with by adding an OR condition
        FieldMapping fieldMappingForPrefix = new FieldMapping(new Field(null, S),
                                                       null,
                                                              fieldMapping.getVirtualIndexName(),
                                                              fieldMapping.getPhysicalIndexName(),
                                                              fieldMapping.getIndexType(),
                                                              fieldMapping.isContextAware());
        AttributeValue physicalValuePrefixAttribute = fieldMapper.apply(fieldMappingForPrefix, new AttributeValue(""));
        String valuePlaceholder = ":___value___";
        request.putExpressionAttributeName(namePlaceholder, hashKey);
        request.putExpressionAttributeValue(valuePlaceholder, physicalValuePrefixAttribute);
        request.setExpression((request.getExpression() != null ? request.getExpression() + " and " : "") +
                "begins_with(" + namePlaceholder + ", " + valuePlaceholder + ")");
    }

    private void applyKeyConditionToField(RequestWrapper request, FieldMapping fieldMapping) {
        String conditionExpression = request.getExpression();
        String virtualAttrName = fieldMapping.getSource().getName();
        Map<String, String> expressionAttrNames = request.getExpressionAttributeNames();
        Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(virtualAttrName)).map(Entry::getKey).findFirst() : Optional.empty();
        String toFind = (keyFieldName.orElse(virtualAttrName)) + " = ";
        int start = conditionExpression.indexOf(toFind);
        if (start != -1) {
            // key is present in filter criteria
            int end = conditionExpression.indexOf(" ", start + toFind.length());
            String virtualValuePlaceholder = conditionExpression.substring(start + toFind.length(), end == -1 ? conditionExpression.length() : end);
            AttributeValue virtualAttr = request.getExpressionAttributeValues().get(virtualValuePlaceholder);
            AttributeValue physicalAttr = fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, virtualAttr) : virtualAttr;
            request.putExpressionAttributeValue(virtualValuePlaceholder, physicalAttr);
            request.putExpressionAttributeName(keyFieldName.get(), fieldMapping.getTarget().getName());
        }
    }

    private boolean queryContainsHashKeyCondition(RequestWrapper request,
                                                  String hashKeyField) { // TODO look for hashkey in literals
        String conditionExpression = request.getExpression();
        if (conditionExpression == null) {
            // no filter criteria
            return false;
        }
        Map<String, String> expressionAttrNames = request.getExpressionAttributeNames();
        Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(hashKeyField)).map(Entry::getKey).findFirst() : Optional.empty();
        String fieldToFind = (keyFieldName.orElse(hashKeyField));
        String toFind = fieldToFind + " = ";
        int start = conditionExpression.indexOf(toFind);
        return start != -1;
    }

    @VisibleForTesting
    interface RequestWrapper {
        String getIndexName();
        Map<String, String> getExpressionAttributeNames();
        void putExpressionAttributeName(String key, String value);
        Map<String, AttributeValue> getExpressionAttributeValues();
        void putExpressionAttributeValue(String key, AttributeValue value);
        String getExpression();
        void setExpression(String expression);
        void setIndexName(String indexName);
        Map<String, Condition> getLegacyExpression();
        void clearLegacyExpression();
    }

    @VisibleForTesting
    static class QueryRequestWrapper implements RequestWrapper {

        private final QueryRequest queryRequest;
        @SuppressWarnings("unchecked")
        QueryRequestWrapper(QueryRequest queryRequest) {
            queryRequest.setExpressionAttributeNames(getMutableMap(queryRequest.getExpressionAttributeNames()));
            queryRequest.setExpressionAttributeValues(getMutableMap(queryRequest.getExpressionAttributeValues()));
            this.queryRequest = queryRequest;
        }

        @Override
        public String getIndexName() {
            return queryRequest.getIndexName();
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return queryRequest.getExpressionAttributeNames();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (queryRequest.getExpressionAttributeNames() == null) {
                queryRequest.setExpressionAttributeNames(new HashMap<>());
            }
            queryRequest.getExpressionAttributeNames().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            return queryRequest.getExpressionAttributeValues();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            if (queryRequest.getExpressionAttributeValues() == null) {
                queryRequest.setExpressionAttributeValues(new HashMap<>());
            }
            queryRequest.getExpressionAttributeValues().put(key, value);
        }

        @Override
        public String getExpression() {
            return queryRequest.getKeyConditionExpression();
        }

        @Override
        public void setExpression(String expression) {
            queryRequest.setKeyConditionExpression(expression);
        }

        @Override
        public void setIndexName(String indexName) {
            queryRequest.setIndexName(indexName);
        }

        @Override
        public Map<String, Condition> getLegacyExpression() {
            return queryRequest.getKeyConditions();
        }

        @Override
        public void clearLegacyExpression() {
            queryRequest.clearKeyConditionsEntries();
        }

    }

    private static class ScanRequestWrapper implements RequestWrapper {
        private final ScanRequest scanRequest;

        @SuppressWarnings("unchecked")
        ScanRequestWrapper(ScanRequest scanRequest) {
            scanRequest.setExpressionAttributeNames(getMutableMap(scanRequest.getExpressionAttributeNames()));
            scanRequest.setExpressionAttributeValues(getMutableMap(scanRequest.getExpressionAttributeValues()));
            this.scanRequest = scanRequest;
        }

        @Override
        public String getIndexName() {
            return scanRequest.getIndexName();
        }

        @Override
        public Map<String, String> getExpressionAttributeNames() {
            return scanRequest.getExpressionAttributeNames();
        }

        @Override
        public void putExpressionAttributeName(String key, String value) {
            if (scanRequest.getExpressionAttributeNames() == null) {
                scanRequest.setExpressionAttributeNames(new HashMap<>());
            }
            scanRequest.getExpressionAttributeNames().put(key, value);
        }

        @Override
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            return scanRequest.getExpressionAttributeValues();
        }

        @Override
        public void putExpressionAttributeValue(String key, AttributeValue value) {
            if (scanRequest.getExpressionAttributeValues() == null) {
                scanRequest.setExpressionAttributeValues(new HashMap<>());
            }
            scanRequest.getExpressionAttributeValues().put(key, value);
        }

        @Override
        public String getExpression() {
            return scanRequest.getFilterExpression();
        }

        @Override
        public void setExpression(String expression) {
            scanRequest.setFilterExpression(expression);
        }

        @Override
        public void setIndexName(String indexName) {
            scanRequest.setIndexName(indexName);
        }

        @Override
        public Map<String, Condition> getLegacyExpression() {
            return scanRequest.getScanFilter();
        }

        @Override
        public void clearLegacyExpression() {
            scanRequest.clearScanFilterEntries();
        }

    }

    private static Map getMutableMap(Map potentiallyImmutableMap) {
        return (potentiallyImmutableMap == null) ? null : new HashMap<Object, Object>(potentiallyImmutableMap);
    }

    /*
     * Validate that there are keyConditions or a keyConditionExpression, but not both.
     */
    private void validateQueryRequest(QueryRequest queryRequest) {
        boolean hasKeyConditionExpression = !isEmpty(queryRequest.getKeyConditionExpression());
        boolean hasKeyConditions = (queryRequest.getKeyConditions() != null && queryRequest.getKeyConditions().keySet().size() > 0);
        checkArgument(hasKeyConditionExpression || hasKeyConditions,
                "keyConditionExpression or keyConditions are required");
        checkArgument(!hasKeyConditionExpression || !hasKeyConditions,
                "ambiguous QueryRequest: both keyConditionExpression and keyConditions were provided");
    }

    private void validateScanRequest(ScanRequest scanRequest) {
        boolean hasFilterExpression = !isEmpty(scanRequest.getFilterExpression());
        boolean hasScanFilter = (scanRequest.getScanFilter() != null && scanRequest.getScanFilter().keySet().size() > 0);
        checkArgument(!hasFilterExpression || !hasScanFilter,
                "ambiguous ScanRequest: both filterExpression and scanFilter were provided");
    }

    /*
     * Converts QueryRequest's containing keyConditions to keyConditionExpression and ScanRequest's containing scanFilters.
     * According to the DynamoDB docs, QueryRequest keyConditions and ScanRequest scanFilter's are considered 'legacy parameters'.
     * However, since we support them by converting them to keyConditionExpressions and filterExpression's respectively
     * because they are used by the DynamoDB document API
     * (https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/document/DynamoDB.html).
     */
    private void convertLegacyExpression(RequestWrapper request) {
        if ((request.getLegacyExpression() != null && request.getLegacyExpression().keySet().size() > 0)) {
            List<String> keyConditionExpressionParts = new ArrayList<>();
            AtomicInteger counter = new AtomicInteger(1);
            request.getLegacyExpression().forEach((key, condition) -> {
                checkArgument(ComparisonOperator.valueOf(condition.getComparisonOperator()) == EQ,
                        "unsupported comparison operator " + condition.getComparisonOperator() + " in condition=" + condition);
                checkArgument(condition.getAttributeValueList().size() == 1,
                        "keyCondition with more than one(" + condition.getAttributeValueList().size() + ") encountered in condition=" + condition);
                String field = "#field" + counter;
                String value = ":value" + counter.getAndIncrement();
                keyConditionExpressionParts.add(field + " = " + value);
                request.putExpressionAttributeName(field, key);
                request.putExpressionAttributeValue(value, condition.getAttributeValueList().get(0));
            });
            request.setExpression(Joiner.on(" AND ").join(keyConditionExpressionParts));
            request.clearLegacyExpression();
        }
    }

}