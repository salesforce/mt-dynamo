/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextFieldPlaceholder;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MappingUtils.getNextValuePlaceholder;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RequestWrapper.AbstractRequestWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

abstract class AbstractQueryAndScanMapper implements QueryAndScanMapper {

    private final Function<String, RequestIndex> indexLookup;
    private final ConditionMapper conditionMapper;
    private final ItemMapper itemMapper;

    AbstractQueryAndScanMapper(Function<String, RequestIndex> indexLookup,
                               ConditionMapper conditionMapper,
                               ItemMapper itemMapper) {
        this.indexLookup = indexLookup;
        this.conditionMapper = conditionMapper;
        this.itemMapper = itemMapper;
    }

    @Override
    public void apply(QueryRequest queryRequest) {
        validateQueryRequest(queryRequest);
        convertLegacyExpression(new QueryLegacyConditionRequestWrapper(queryRequest), queryRequest.getKeyConditions());

        // map index name if needed
        RequestIndex requestIndex = getRequestIndexAndMapIndexName(new QueryRequestWrapper(queryRequest, null, null));

        // map key condition
        QueryRequestWrapper keyConditionExpressionRequest = new QueryRequestWrapper(queryRequest,
            queryRequest::getKeyConditionExpression, queryRequest::setKeyConditionExpression);
        conditionMapper.applyToKeyCondition(keyConditionExpressionRequest, requestIndex);

        // map filter expression if there is one
        QueryRequestWrapper filterExpressionRequest = new QueryRequestWrapper(queryRequest,
            queryRequest::getFilterExpression, queryRequest::setFilterExpression);
        applyToFilterExpression(filterExpressionRequest);

        // map exclusive start key if there is one
        applyExclusiveStartKey(filterExpressionRequest, requestIndex);
    }

    @Override
    public ScanResult executeScan(AmazonDynamoDB amazonDynamoDB, ScanRequest scanRequest) {
        validateScanRequest(scanRequest);
        convertLegacyExpression(new ScanLegacyConditionRequestWrapper(scanRequest), scanRequest.getScanFilter());

        return executeScanInternal(amazonDynamoDB, scanRequest);
    }

    abstract ScanResult executeScanInternal(AmazonDynamoDB amazonDynamoDB, ScanRequest scanRequest);

    protected RequestIndex getRequestIndexAndMapIndexName(QueryOrScanRequestWrapper request) {
        RequestIndex requestIndex = indexLookup.apply(request.getIndexName());
        if (requestIndex.getVirtualSecondaryIndex().isPresent()) {
            request.setIndexName(requestIndex.getPhysicalSecondaryIndex().get().getIndexName());
        }
        return requestIndex;
    }

    protected void applyToFilterExpression(QueryOrScanRequestWrapper request) {
        if (request.getExpression() != null) {
            conditionMapper.applyToFilterExpression(request);
        }
    }

    protected void applyExclusiveStartKey(QueryOrScanRequestWrapper request, RequestIndex requestIndex) {
        Map<String, AttributeValue> virtualStartKey = request.getExclusiveStartKey();
        if (virtualStartKey != null) {
            Map<String, AttributeValue> physicalStartKey = itemMapper.applyToKeyAttributes(virtualStartKey,
                requestIndex.getVirtualSecondaryIndex().orElse(null));
            request.setExclusiveStartKey(physicalStartKey);
        }
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

    private enum LegacyKeyConditionType {
        EQ((field, value) -> field + " = " + value),
        GT((field, value) -> field + " > " + value),
        GE((field, value) -> field + " >= " + value),
        LT((field, value) -> field + " < " + value),
        LE((field, value) -> field + " <= " + value),
        BETWEEN(2, (field, values) -> field + " BETWEEN " + values.get(0) + " AND " + values.get(1));

        final int numValues;
        final BiFunction<String, List<String>, String> toExpression;

        LegacyKeyConditionType(BiFunction<String, String, String> toExpression) {
            this(1, (field, values) -> toExpression.apply(field, values.get(0)));
        }

        LegacyKeyConditionType(int numValues, BiFunction<String, List<String>, String> toExpression) {
            this.numValues = numValues;
            this.toExpression = toExpression;
        }
    }

    private static final Map<ComparisonOperator, LegacyKeyConditionType> KEY_CONDITION_PER_OPERATOR = ImmutableMap
        .<ComparisonOperator, LegacyKeyConditionType>builder()
        .put(ComparisonOperator.EQ, LegacyKeyConditionType.EQ)
        .put(ComparisonOperator.GT, LegacyKeyConditionType.GT)
        .put(ComparisonOperator.GE, LegacyKeyConditionType.GE)
        .put(ComparisonOperator.LT, LegacyKeyConditionType.LT)
        .put(ComparisonOperator.LE, LegacyKeyConditionType.LE)
        .put(ComparisonOperator.BETWEEN, LegacyKeyConditionType.BETWEEN)
        .build();

    /*
     * Converts QueryRequest objects containing keyConditions to keyConditionExpression and ScanRequest objects
     * containing scanFilters.  According to the DynamoDB docs, QueryRequest keyConditions and ScanRequest scanFilter
     * fields are considered 'legacy parameters'.  However, we support them by converting them to keyConditionExpression
     * and filterExpression fields respectively because they are used by the DynamoDB document API
     * (https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/document/DynamoDB.html).
     */
    private void convertLegacyExpression(RequestWrapper request, Map<String, Condition> legacyConditions) {
        if (legacyConditions != null && !legacyConditions.isEmpty()) {
            List<String> keyConditionExpressionParts = new ArrayList<>();
            legacyConditions.forEach((key, condition) -> {
                final ComparisonOperator comparisonOperator = ComparisonOperator
                    .valueOf(condition.getComparisonOperator());
                LegacyKeyConditionType type = KEY_CONDITION_PER_OPERATOR.get(comparisonOperator);
                checkNotNull(type,"unsupported comparison operator " + condition.getComparisonOperator()
                    + " in condition=" + condition);
                checkArgument(condition.getAttributeValueList().size() == type.numValues,
                    "keyCondition with wrong number of attribute values (" + condition.getAttributeValueList().size()
                        + ") encountered in condition=" + condition);

                String fieldPlaceholder = getNextFieldPlaceholder(request);
                request.putExpressionAttributeName(fieldPlaceholder, key);

                List<String> valuePlaceholders = new ArrayList<>(condition.getAttributeValueList().size());
                condition.getAttributeValueList().forEach(value -> {
                    String valuePlaceholder = getNextValuePlaceholder(request);
                    request.putExpressionAttributeValue(valuePlaceholder, value);
                    valuePlaceholders.add(valuePlaceholder);
                });

                String expression = type.toExpression.apply(fieldPlaceholder, valuePlaceholders);
                keyConditionExpressionParts.add(expression);
            });
            request.setExpression(Joiner.on(" AND ").join(keyConditionExpressionParts));
        }
    }

    interface QueryOrScanRequestWrapper extends RequestWrapper {

        Map<String, AttributeValue> getExclusiveStartKey();

        void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey);

        String getIndexName();

        void setIndexName(String indexName);
    }

    static class QueryRequestWrapper extends AbstractRequestWrapper implements QueryOrScanRequestWrapper {

        private final QueryRequest queryRequest;

        QueryRequestWrapper(QueryRequest queryRequest, Supplier<String> getExpression, Consumer<String> setExpression) {
            super(queryRequest::getExpressionAttributeNames, queryRequest::setExpressionAttributeNames,
                queryRequest::getExpressionAttributeValues, queryRequest::setExpressionAttributeValues,
                getExpression, setExpression);
            this.queryRequest = queryRequest;
        }

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            return queryRequest.getExclusiveStartKey();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            queryRequest.setExclusiveStartKey(exclusiveStartKey);
        }

        @Override
        public String getIndexName() {
            return queryRequest.getIndexName();
        }

        @Override
        public void setIndexName(String indexName) {
            queryRequest.setIndexName(indexName);
        }
    }

    private static class QueryLegacyConditionRequestWrapper extends QueryRequestWrapper {

        QueryLegacyConditionRequestWrapper(QueryRequest queryRequest) {
            super(queryRequest, null,
                exp -> {
                    queryRequest.setKeyConditionExpression(exp);
                    queryRequest.clearKeyConditionsEntries();
                });
        }
    }

    static class ScanRequestWrapper extends AbstractRequestWrapper implements QueryOrScanRequestWrapper {

        private final ScanRequest scanRequest;

        ScanRequestWrapper(ScanRequest scanRequest, Supplier<String> getExpression, Consumer<String> setExpression) {
            super(scanRequest::getExpressionAttributeNames, scanRequest::setExpressionAttributeNames,
                scanRequest::getExpressionAttributeValues, scanRequest::setExpressionAttributeValues,
                getExpression, setExpression);
            this.scanRequest = scanRequest;
        }

        @Override
        public Map<String, AttributeValue> getExclusiveStartKey() {
            return scanRequest.getExclusiveStartKey();
        }

        @Override
        public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
            scanRequest.setExclusiveStartKey(exclusiveStartKey);
        }

        @Override
        public String getIndexName() {
            return scanRequest.getIndexName();
        }

        @Override
        public void setIndexName(String indexName) {
            scanRequest.setIndexName(indexName);
        }
    }

    private static class ScanLegacyConditionRequestWrapper extends ScanRequestWrapper {

        ScanLegacyConditionRequestWrapper(ScanRequest scanRequest) {
            super(scanRequest, null,
                exp -> {
                    scanRequest.setFilterExpression(exp);
                    scanRequest.clearScanFilterEntries();
                });
        }
    }
}
