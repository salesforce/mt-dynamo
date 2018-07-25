/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

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

/*
 * Maps query and scan requests against virtual tables to their physical table counterpart according to the provided
 * TableMapping, delegating field mapping to the provided FieldMapper.
 *
 * @author msgroi
 */
class QueryMapper {

    private static final String NAME_PLACEHOLDER = "#___name___";
    private static final String VALUE_PLACEHOLDER = ":___value___";

    private final FieldMapper fieldMapper;
    private final TableMapping tableMapping;

    QueryMapper(TableMapping tableMapping, FieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.tableMapping = tableMapping;
    }

    /*
     * Takes a QueryRequest representing a query against a virtual table and mutates it so it can be applied to its
     * physical table counterpart.
     */
    void apply(QueryRequest queryRequest) {
        validateQueryRequest(queryRequest);
        apply(new QueryRequestWrapper(queryRequest));
    }

    /*
     * Takes a ScanRequest representing a scan against a virtual table and mutates it so it can be applied to its
     * physical table counterpart.
     */
    void apply(ScanRequest scanRequest) {
        validateScanRequest(scanRequest);
        apply(new ScanRequestWrapper(scanRequest));
    }

    private void apply(RequestWrapper request) {
        convertLegacyExpression(request);
        applyKeyCondition(request);
        applyExclusiveStartKey(request);
    }

    private void applyKeyCondition(RequestWrapper request) {
        String virtualHashKey;
        Collection<FieldMapping> fieldMappings;

        if (request.getIndexName() == null) {
            // query or scan does NOT use index
            virtualHashKey = tableMapping.getVirtualTable().getPrimaryKey().getHashKey();
            Map<String, List<FieldMapping>> fieldMappingDeduped = tableMapping.getAllVirtualToPhysicalFieldMappings();
            fieldMappings = dedupeFieldMappings(fieldMappingDeduped).values();
        } else {
            // query uses index
            DynamoSecondaryIndex virtualSecondaryIndex = tableMapping.getVirtualTable().findSi(request.getIndexName());
            fieldMappings = tableMapping.getIndexPrimaryKeyFieldMappings(virtualSecondaryIndex);
            request.setIndexName(((List<FieldMapping>) fieldMappings).get(0).getPhysicalIndexName());
            virtualHashKey = virtualSecondaryIndex.getPrimaryKey().getHashKey();
        }

        convertFieldNameLiteralsToExpressionNames(fieldMappings, request);

        if (!queryContainsHashKeyCondition(request, virtualHashKey)) {
            /*
             * the expression does not contain the table or index key that's being used in the
             * query, add begins_with clause
             */
            String physicalHashKey = fieldMappings.stream().filter((Predicate<FieldMapping>) fieldMapping ->
                fieldMapping.getSource().getName().equals(virtualHashKey)).findFirst()
                .orElseThrow((Supplier<IllegalArgumentException>) () ->
                    new IllegalArgumentException("field mapping not found hashkey field " + virtualHashKey)).getTarget()
                .getName();
            FieldMapping fieldMapping = fieldMappings.stream().filter((Predicate<FieldMapping>) fieldMapping1 ->
                fieldMapping1.getSource().getName().equals(virtualHashKey)).findFirst()
                .orElseThrow((Supplier<IllegalArgumentException>) () ->
                    new IllegalArgumentException("field mapping not found hashkey field " + virtualHashKey));
            addBeginsWith(request, physicalHashKey, fieldMapping);
        }

        checkNotNull(request.getPrimaryExpression(), "request expression is required");

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
     * It is an effective no-op, meaning, there are no duplicates to remove, except when a scan is performed against
     * a table that maps a given virtual field to multiple physical fields.  In that case, it doesn't matter which
     * field we use in the query, the results should be the same, so we choose one of the physical fields arbitrarily.
     */
    private Map<String, FieldMapping> dedupeFieldMappings(Map<String, List<FieldMapping>> fieldMappings) {
        return fieldMappings.entrySet().stream().collect(Collectors.toMap(
            Entry::getKey,
            fieldMappingEntry -> fieldMappingEntry.getValue().get(0)
        ));
    }

    private void addBeginsWith(RequestWrapper request, String hashKey, FieldMapping fieldMapping) {
        /*
         * TODO make sure it properly identifies that it doesn't need to add this ... make sure it's an equals
         * condition and that the equals condition can't be hacked ... make sure you can't negate the begins_with
         * by adding an OR condition
         */
        FieldMapping fieldMappingForPrefix = new FieldMapping(new Field(null, S),
            null,
            fieldMapping.getVirtualIndexName(),
            fieldMapping.getPhysicalIndexName(),
            fieldMapping.getIndexType(),
            fieldMapping.isContextAware());
        AttributeValue physicalValuePrefixAttribute = fieldMapper.apply(fieldMappingForPrefix, new AttributeValue(""));
        request.putExpressionAttributeName(NAME_PLACEHOLDER, hashKey);
        request.putExpressionAttributeValue(VALUE_PLACEHOLDER, physicalValuePrefixAttribute);
        request.setPrimaryExpression(
            (request.getPrimaryExpression() != null ? request.getPrimaryExpression() + " and " : "")
                + "begins_with(" + NAME_PLACEHOLDER + ", " + VALUE_PLACEHOLDER + ")");
    }

    private void applyKeyConditionToField(RequestWrapper request, FieldMapping fieldMapping) {
        applyKeyConditionToField(request, fieldMapping, request.getPrimaryExpression(), request.getFilterExpression());
    }

    /*
     * Finds a virtual field name reference in the expression attribute names, finds the value in the right-hand side
     * operand in the primary expression or filter expression, gets its value in the expression attribute values,
     * applies the multi-mapping value mapping, and sets the physical name of the field to that of the target field.
     */
    private void applyKeyConditionToField(RequestWrapper request,
        FieldMapping fieldMapping,
        String primaryExpression,
        String filterExpression) {
        if (primaryExpression != null) {
            String virtualAttrName = fieldMapping.getSource().getName();
            Map<String, String> expressionAttrNames = request.getExpressionAttributeNames();
            Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
                .filter(entry -> entry.getValue().equals(virtualAttrName)).map(Entry::getKey).findAny()
                : Optional.empty();
            if (keyFieldName.isPresent() && !keyFieldName.get().equals(NAME_PLACEHOLDER)) {
                String virtualValuePlaceholder = findVirtualValuePlaceholder(primaryExpression, filterExpression,
                    keyFieldName.get());
                AttributeValue virtualAttr = request.getExpressionAttributeValues().get(virtualValuePlaceholder);
                AttributeValue physicalAttr =
                    fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, virtualAttr) : virtualAttr;
                request.putExpressionAttributeValue(virtualValuePlaceholder, physicalAttr);
                request.putExpressionAttributeName(keyFieldName.get(), fieldMapping.getTarget().getName());
            }
        }
    }

    private void applyExclusiveStartKey(RequestWrapper request) {
        Map<String, AttributeValue> key = request.getExclusiveStartKey();
        if (key != null) {
            request.setExclusiveStartKey(tableMapping.getItemMapper().apply(request.getExclusiveStartKey()));
        }
    }

    /*
     * Finds the value in the right-hand side operand where the left-hand operator is a given field, first in the
     * primary expression, then in the filterExpression.
     */
    private String findVirtualValuePlaceholder(String primaryExpression, String filterExpression, String keyFieldName) {
        return findVirtualValuePlaceholder(primaryExpression, keyFieldName)
            .orElseGet((Supplier<String>) () -> findVirtualValuePlaceholder(filterExpression, keyFieldName)
                .orElseThrow((Supplier<IllegalArgumentException>) () ->
                    new IllegalArgumentException("field " + keyFieldName + " not found in either conditionExpression="
                        + primaryExpression + ", or filterExpression=" + filterExpression)));
    }

    /*
     * Finds the value in the right-hand side operand of an expression where the left-hand operator is a given field.
     */
    private Optional<String> findVirtualValuePlaceholder(String conditionExpression, String keyFieldName) {
        String toFind = keyFieldName + " = ";
        int start = conditionExpression.indexOf(toFind);
        if (start == -1) {
            return Optional.empty();
        }
        int end = conditionExpression.indexOf(" ", start + toFind.length());
        return Optional.of(conditionExpression.substring(start + toFind.length(),
            end == -1 ? conditionExpression.length() : end)); // TODO add support for non-EQ operators
    }

    private boolean queryContainsHashKeyCondition(RequestWrapper request,
        String hashKeyField) { // TODO look for hashkey in literals
        String conditionExpression = request.getPrimaryExpression();
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

        String getPrimaryExpression();

        void setPrimaryExpression(String expression);

        String getFilterExpression();

        void setFilterExpression(String s);

        void setIndexName(String indexName);

        Map<String, Condition> getLegacyExpression();

        void clearLegacyExpression();

        Map<String, AttributeValue> getExclusiveStartKey();

        void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey);
    }

    @VisibleForTesting
    static class QueryRequestWrapper implements RequestWrapper {

        private final QueryRequest queryRequest;

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
        public String getPrimaryExpression() {
            return queryRequest.getKeyConditionExpression();
        }

        @Override
        public void setPrimaryExpression(String expression) {
            queryRequest.setKeyConditionExpression(expression);
        }

        @Override
        public String getFilterExpression() {
            return queryRequest.getFilterExpression();
        }

        @Override
        public void setFilterExpression(String filterExpression) {
            queryRequest.setFilterExpression(filterExpression);
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

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            return queryRequest.getExclusiveStartKey();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            queryRequest.setExclusiveStartKey(exclusiveStartKey);
        }

    }

    private static class ScanRequestWrapper implements RequestWrapper {

        private final ScanRequest scanRequest;

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
        public String getPrimaryExpression() {
            return scanRequest.getFilterExpression();
        }

        @Override
        public void setPrimaryExpression(String expression) {
            scanRequest.setFilterExpression(expression);
        }

        @Override
        public String getFilterExpression() {
            return null;
        }

        @Override
        public void setFilterExpression(String filterExpression) {
            if (filterExpression != null) {
                throw new UnsupportedOperationException();
            }
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

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            return scanRequest.getExclusiveStartKey();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            scanRequest.setExclusiveStartKey(exclusiveStartKey);
        }

    }

    private static <K, V> Map<K, V> getMutableMap(Map<K, V> potentiallyImmutableMap) {
        return (potentiallyImmutableMap == null) ? null : new HashMap<>(potentiallyImmutableMap);
    }

    /*
     * Validate that there are keyConditions or a keyConditionExpression, but not both.
     */
    private void validateQueryRequest(QueryRequest queryRequest) {
        boolean hasKeyConditionExpression = !isEmpty(queryRequest.getKeyConditionExpression());
        boolean hasKeyConditions = (queryRequest.getKeyConditions() != null
            && queryRequest.getKeyConditions().keySet().size() > 0);
        checkArgument(hasKeyConditionExpression || hasKeyConditions,
            "keyConditionExpression or keyConditions are required");
        checkArgument(!hasKeyConditionExpression || !hasKeyConditions,
            "ambiguous QueryRequest: both keyConditionExpression and keyConditions were provided");
    }

    private void validateScanRequest(ScanRequest scanRequest) {
        boolean hasFilterExpression = !isEmpty(scanRequest.getFilterExpression());
        boolean hasScanFilter = (scanRequest.getScanFilter() != null
            && scanRequest.getScanFilter().keySet().size() > 0);
        checkArgument(!hasFilterExpression || !hasScanFilter,
            "ambiguous ScanRequest: both filterExpression and scanFilter were provided");
    }

    /*
     * Converts QueryRequest's containing keyConditions to keyConditionExpression and ScanRequest's containing
     * scanFilters.  According to the DynamoDB docs, QueryRequest keyConditions and ScanRequest scanFilter's are
     * considered 'legacy parameters'.  However, since we support them by converting them to keyConditionExpressions
     * and filterExpression's respectively because they are used by the DynamoDB document API
     * (https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/document/DynamoDB.html).
     */
    private void convertLegacyExpression(RequestWrapper request) {
        if ((request.getLegacyExpression() != null && request.getLegacyExpression().keySet().size() > 0)) {
            List<String> keyConditionExpressionParts = new ArrayList<>();
            AtomicInteger counter = new AtomicInteger(1);
            request.getLegacyExpression().forEach((key, condition) -> {
                checkArgument(ComparisonOperator.valueOf(condition.getComparisonOperator()) == EQ,
                    "unsupported comparison operator " + condition.getComparisonOperator() + " in condition="
                        + condition);
                checkArgument(condition.getAttributeValueList().size() == 1,
                    "keyCondition with more than one(" + condition.getAttributeValueList().size()
                        + ") encountered in condition=" + condition);
                String field = "#field" + counter;
                String value = ":value" + counter.getAndIncrement();
                keyConditionExpressionParts.add(field + " = " + value);
                request.putExpressionAttributeName(field, key);
                request.putExpressionAttributeValue(value, condition.getAttributeValueList().get(0));
            });
            request.setPrimaryExpression(Joiner.on(" AND ").join(keyConditionExpressionParts));
            request.clearLegacyExpression();
        }
    }

    @VisibleForTesting
    void convertFieldNameLiteralsToExpressionNames(Collection<FieldMapping> fieldMappings,
        RequestWrapper request) {
        request.setPrimaryExpression(
            convertFieldNameLiteralsToExpressionNames(fieldMappings, request.getPrimaryExpression(), request));
        request.setFilterExpression(
            convertFieldNameLiteralsToExpressionNames(fieldMappings, request.getFilterExpression(), request));
    }

    private String convertFieldNameLiteralsToExpressionNames(Collection<FieldMapping> fieldMappings,
        String conditionExpression,
        RequestWrapper request) {
        String newConditionExpression = conditionExpression;
        if (conditionExpression != null) {
            AtomicInteger counter = new AtomicInteger(1);
            for (FieldMapping fieldMapping : fieldMappings) {
                String virtualFieldName = fieldMapping.getSource().getName();
                String toFind = " " + virtualFieldName + " =";
                int start = (" " + newConditionExpression).indexOf(toFind); // TODO add support for non-EQ operators
                while (start >= 0) {
                    String fieldLiteral = newConditionExpression.substring(start, start + virtualFieldName.length());
                    String fieldPlaceholder = getNextFieldPlaceholder(request.getExpressionAttributeNames(), counter);
                    newConditionExpression = request.getPrimaryExpression()
                        .replaceAll(fieldLiteral + " ", fieldPlaceholder + " ");
                    request.putExpressionAttributeName(fieldPlaceholder, fieldLiteral);
                    start = (" " + newConditionExpression).indexOf(toFind);
                }
            }
        }
        return newConditionExpression;
    }

    private String getNextFieldPlaceholder(Map<String, String> expressionAttributeNames, AtomicInteger counter) {
        String fieldPlaceholderCandidate = "#field" + counter.get();
        while (expressionAttributeNames != null && expressionAttributeNames.containsKey(fieldPlaceholderCandidate)) {
            fieldPlaceholderCandidate = "#field" + counter.incrementAndGet();
        }
        return fieldPlaceholderCandidate;
    }

}