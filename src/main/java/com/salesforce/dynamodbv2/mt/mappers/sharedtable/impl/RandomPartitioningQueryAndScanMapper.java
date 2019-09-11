/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.ConditionMapper.NAME_PLACEHOLDER;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/**
 * {@linkn QueryAndScanMapper} implementation for shared tables using random partitioning.
 *
 * @author msgroi
 */
class QueryAndScanMapper implements IQueryAndScanMapper {

    private static final String VALUE_PLACEHOLDER = ":___value___";
    private static final Map<ComparisonOperator, BiFunction<String, String, String>>
            FIELD_AND_VALUE_TO_EXPRESSION_STRINGS
        = new ImmutableMap.Builder<ComparisonOperator, BiFunction<String, String, String>>()
        .put(EQ, (field, value) -> field + " = " + value)
        .put(GT, (field, value) -> field + " > " + value)
        .put(GE, (field, value) -> field + " >= " + value)
        .put(LT, (field, value) -> field + " < " + value)
        .put(LE, (field, value) -> field + " <= " + value)
        .build();
    private static final Set<ComparisonOperator> SUPPORTED_COMPARISON_OPERATORS = FIELD_AND_VALUE_TO_EXPRESSION_STRINGS
        .keySet();

    private final FieldMapper fieldMapper;
    private final RandomPartitioningTableMapping tableMapping;

    QueryAndScanMapper(RandomPartitioningTableMapping tableMapping, FieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.tableMapping = tableMapping;
    }

    @Override
    public void apply(QueryRequest queryRequest) {
        validateQueryRequest(queryRequest);
        apply(new QueryRequestWrapper(queryRequest));
    }

    @Override
    public void apply(ScanRequest scanRequest) {
        validateScanRequest(scanRequest);
        apply(new ScanRequestWrapper(scanRequest));
    }

    private void apply(RequestWrapper request) {
        convertLegacyExpression(request);

        DynamoSecondaryIndex virtualSecondaryIndex = null;
        String virtualHashKey;
        Collection<FieldMapping> fieldMappings;
        if (request.getIndexName() == null) {
            // query or scan does NOT use index
            virtualHashKey = tableMapping.getVirtualTable().getPrimaryKey().getHashKey();
            fieldMappings = tableMapping.getTablePrimaryKeyFieldMappings();
        } else {
            // query uses index
            virtualSecondaryIndex = tableMapping.getVirtualTable().findSi(request.getIndexName());
            fieldMappings = tableMapping.getIndexPrimaryKeyFieldMappings(virtualSecondaryIndex);
            request.setIndexName(((List<FieldMapping>) fieldMappings).get(0).getPhysicalIndexName());
            virtualHashKey = virtualSecondaryIndex.getPrimaryKey().getHashKey();
        }

        // if the key condition expression does not contain the table or index key being used, add begins_with clause
        addBeginsWithIfNeeded(request, virtualHashKey, fieldMappings);

        // map each field to its target name and apply field prefixing as appropriate
        tableMapping.getConditionMapper().apply(request, virtualSecondaryIndex);

        applyExclusiveStartKey(request, virtualSecondaryIndex);
    }

    private void addBeginsWithIfNeeded(RequestWrapper request, String virtualHashKey,
                                       Collection<FieldMapping> fieldMappings) {
        if (!queryContainsHashKeyCondition(request, virtualHashKey)) {
            FieldMapping fieldMapping = fieldMappings.stream()
                .filter(fieldMapping1 -> fieldMapping1.getSource().getName().equals(virtualHashKey))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "field mapping not found hash-key field " + virtualHashKey));
            Field physicalHashKey = fieldMapping.getTarget();
            addBeginsWith(request, physicalHashKey, fieldMapping);
        }
    }

    private void addBeginsWith(RequestWrapper request, Field hashKey, FieldMapping fieldMapping) {
        /*
         * TODO make sure it properly identifies that it doesn't need to add this ... make sure it's an equals
         * condition and that the equals condition can't be hacked ... make sure you can't negate the begins_with
         * by adding an OR condition
         */
        FieldMapping fieldMappingForPrefix = new FieldMapping(new Field(null, S),
            new Field(null, hashKey.getType()),
            fieldMapping.getVirtualIndexName(),
            fieldMapping.getPhysicalIndexName(),
            fieldMapping.getIndexType(),
            fieldMapping.isContextAware());
        AttributeValue physicalValuePrefixAttribute = fieldMapper.apply(fieldMappingForPrefix, new AttributeValue(""));
        request.putExpressionAttributeName(NAME_PLACEHOLDER, hashKey.getName());
        request.putExpressionAttributeValue(VALUE_PLACEHOLDER, physicalValuePrefixAttribute);
        request.setPrimaryExpression(
            (request.getPrimaryExpression() != null ? request.getPrimaryExpression() + " and " : "")
                + "begins_with(" + NAME_PLACEHOLDER + ", " + VALUE_PLACEHOLDER + ")");
    }

    private void applyExclusiveStartKey(RequestWrapper request, @Nullable DynamoSecondaryIndex virtualSecondaryIndex) {
        Map<String, AttributeValue> virtualStartKey = request.getExclusiveStartKey();
        if (virtualStartKey != null) {
            request.setExclusiveStartKey(
                tableMapping.getItemMapper().applyToKeyAttributes(virtualStartKey, virtualSecondaryIndex));
        }
    }

    private boolean queryContainsHashKeyCondition(RequestWrapper request,
        String hashKeyField) { // TODO look for hash key in literals
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
        boolean hasKeyConditionExpression = !Strings.isNullOrEmpty(queryRequest.getKeyConditionExpression());
        boolean hasKeyConditions = (queryRequest.getKeyConditions() != null
            && !queryRequest.getKeyConditions().keySet().isEmpty());
        checkArgument(hasKeyConditionExpression || hasKeyConditions,
            "keyConditionExpression or keyConditions are required");
        checkArgument(!hasKeyConditionExpression || !hasKeyConditions,
            "ambiguous QueryRequest: both keyConditionExpression and keyConditions were provided");
    }

    private void validateScanRequest(ScanRequest scanRequest) {
        boolean hasFilterExpression = !Strings.isNullOrEmpty(scanRequest.getFilterExpression());
        boolean hasScanFilter = (scanRequest.getScanFilter() != null
            && !scanRequest.getScanFilter().keySet().isEmpty());
        checkArgument(!hasFilterExpression || !hasScanFilter,
            "ambiguous ScanRequest: both filterExpression and scanFilter were provided");
    }

    /*
     * Converts QueryRequest objects containing keyConditions to keyConditionExpression and ScanRequest objects
     * containing scanFilters.  According to the DynamoDB docs, QueryRequest keyConditions and ScanRequest scanFilter
     * fields are considered 'legacy parameters'.  However, we support them by converting them to keyConditionExpression
     * and filterExpression fields respectively because they are used by the DynamoDB document API
     * (https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/document/DynamoDB.html).
     */
    private void convertLegacyExpression(RequestWrapper request) {
        if ((request.getLegacyExpression() != null && !request.getLegacyExpression().keySet().isEmpty())) {
            List<String> keyConditionExpressionParts = new ArrayList<>();
            AtomicInteger counter = new AtomicInteger(1);
            request.getLegacyExpression().forEach((key, condition) -> {
                final ComparisonOperator comparisonOperator = ComparisonOperator
                    .valueOf(condition.getComparisonOperator());
                checkArgument(SUPPORTED_COMPARISON_OPERATORS.contains(comparisonOperator),
                    "unsupported comparison operator " + condition.getComparisonOperator() + " in condition="
                        + condition);
                checkArgument(condition.getAttributeValueList().size() == 1,
                    "keyCondition with more than one (" + condition.getAttributeValueList().size()
                        + ") attribute value encountered in condition=" + condition);
                String field = "#field" + counter;
                String value = ":value" + counter.getAndIncrement();
                keyConditionExpressionParts
                    .add(FIELD_AND_VALUE_TO_EXPRESSION_STRINGS.get(comparisonOperator).apply(field, value));
                request.putExpressionAttributeName(field, key);
                request.putExpressionAttributeValue(value, condition.getAttributeValueList().get(0));
            });
            request.setPrimaryExpression(Joiner.on(" AND ").join(keyConditionExpressionParts));
            request.clearLegacyExpression();
        }
    }

}