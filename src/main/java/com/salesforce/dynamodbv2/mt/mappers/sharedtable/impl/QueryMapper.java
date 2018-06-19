/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkNotNull;

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
        checkNotNull(queryRequest.getKeyConditionExpression(), "only QueryRequest's with keyConditionExpressions are currently supported");
        applyKeyCondition(new QueryRequestWrapper(queryRequest));
    }

    /*
     * Takes a ScanRequest representing a scan against a virtual table and mutates it so it can be applied to its physical table counterpart.
     */
    void apply(ScanRequest scanRequest) {
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
        // make sure it properly identifies that it doesn't need to add this ... make sure it's an equals condition and that the equals condition can't be hacked
        FieldMapping fieldMappingForPrefix = new FieldMapping(new Field(null, S),
                                                       null,
                                                              fieldMapping.getVirtualIndexName(),
                                                              fieldMapping.getPhysicalIndexName(),
                                                              fieldMapping.getIndexType(),
                                                              fieldMapping.isContextAware());
        AttributeValue physicalValuePrefixAttribute = fieldMapper.apply(fieldMappingForPrefix, new AttributeValue(""));
        String valuePlaceholder = ":___value___";
        request.getExpressionAttributeNames().put(namePlaceholder, hashKey);
        request.getExpressionAttributeValues().put(valuePlaceholder, physicalValuePrefixAttribute);
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
            Map<String, AttributeValue> expressionAttrValues = request.getExpressionAttributeValues();
            AttributeValue virtualAttr = expressionAttrValues.get(virtualValuePlaceholder);
            AttributeValue physicalAttr = fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, virtualAttr) : virtualAttr;
            expressionAttrValues.put(virtualValuePlaceholder, physicalAttr);
            if (keyFieldName.isPresent()) {
                expressionAttrNames.put(keyFieldName.get(), fieldMapping.getTarget().getName());
            } else {
                request.setExpression(conditionExpression.replace(toFind, fieldMapping.getTarget().getName() + " = "));
            }
        }
    }

    private boolean queryContainsHashKeyCondition(RequestWrapper request,
                                                  String hashKeyField) {
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

    private interface RequestWrapper {
        String getIndexName();
        Map<String, String> getExpressionAttributeNames();
        Map<String, AttributeValue> getExpressionAttributeValues();
        String getExpression();
        void setExpression(String expression);
        void setIndexName(String indexName);
    }

    private static class QueryRequestWrapper implements RequestWrapper {
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
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            return queryRequest.getExpressionAttributeValues();
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
        public Map<String, AttributeValue> getExpressionAttributeValues() {
            return scanRequest.getExpressionAttributeValues();
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
    }

    private static Map getMutableMap(Map potentiallyImmutableMap) {
        return (potentiallyImmutableMap == null) ? null : new HashMap<Object, Object>(potentiallyImmutableMap);
    }

}