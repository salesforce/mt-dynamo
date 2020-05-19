/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RandomPartitioningConditionMapper.NAME_PLACEHOLDER;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.annotations.VisibleForTesting;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@linkn QueryAndScanMapper} implementation for shared tables using random partitioning.
 *
 * @author msgroi
 */
class RandomPartitioningQueryAndScanMapper extends AbstractQueryAndScanMapper {

    private static final String VALUE_PLACEHOLDER = ":___value___";

    private final FieldMapper fieldMapper;
    private final List<FieldMapping> tablePrimaryKeyFieldMappings;
    private final Map<DynamoSecondaryIndex, List<FieldMapping>> indexPrimaryKeyFieldMappings;

    RandomPartitioningQueryAndScanMapper(String context,
                                         Function<String, RequestIndex> indexLookup,
                                         ConditionMapper conditionMapper,
                                         ItemMapper itemMapper,
                                         FieldMapper fieldMapper,
                                         List<FieldMapping> tablePrimaryKeyFieldMappings,
                                         Map<DynamoSecondaryIndex, List<FieldMapping>> indexPrimaryKeyFieldMappings) {
        super(context, indexLookup, conditionMapper, itemMapper);
        this.fieldMapper = fieldMapper;
        this.tablePrimaryKeyFieldMappings = tablePrimaryKeyFieldMappings;
        this.indexPrimaryKeyFieldMappings = indexPrimaryKeyFieldMappings;
    }

    @Override
    ScanResult executeScanInternal(AmazonDynamoDB amazonDynamoDb, ScanRequest scanRequest) {
        // map scan request
        applyToScanInternal(scanRequest);

        // keep moving forward pages until we find at least one record for current tenant or reach end
        ScanResult scanResult;
        while ((scanResult = amazonDynamoDb.scan(scanRequest)).getItems().isEmpty()
            && scanResult.getLastEvaluatedKey() != null) {
            scanRequest.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
        return scanResult;
    }

    @VisibleForTesting
    void applyToScanInternal(ScanRequest scanRequest) {
        ScanRequestWrapper request = new ScanRequestWrapper(scanRequest, scanRequest::getFilterExpression,
            scanRequest::setFilterExpression);

        // map index name if needed
        RequestIndex requestIndex = getRequestIndexAndMapIndexName(new ScanRequestWrapper(scanRequest, null, null));

        // if the filter expression does not contain the table or index key being used, add begins_with clause
        List<FieldMapping> fieldMappings = requestIndex.getVirtualSecondaryIndex().isPresent()
            ? indexPrimaryKeyFieldMappings.get(requestIndex.getVirtualSecondaryIndex().get())
            : tablePrimaryKeyFieldMappings;
        addBeginsWithIfNeeded(request, requestIndex.getVirtualPk().getHashKey(), fieldMappings);

        checkNotNull(request.getExpression());

        // map filter expression
        applyToFilterExpression(request);

        // map exclusive start key if there is one
        applyExclusiveStartKey(request, requestIndex);
    }

    private void addBeginsWithIfNeeded(RequestWrapper request, String virtualHashKey,
                                       Collection<FieldMapping> fieldMappings) {
        if (!scanRequestContainsHashKeyCondition(request, virtualHashKey)) {
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
        AttributeValue physicalValuePrefixAttribute = fieldMapper.apply(
            context,
            fieldMappingForPrefix,
            new AttributeValue(""));
        request.putExpressionAttributeName(NAME_PLACEHOLDER, hashKey.getName());
        request.putExpressionAttributeValue(VALUE_PLACEHOLDER, physicalValuePrefixAttribute);
        request.setExpression(
            (request.getExpression() != null ? request.getExpression() + " and " : "")
                + "begins_with(" + NAME_PLACEHOLDER + ", " + VALUE_PLACEHOLDER + ")");
    }

    private boolean scanRequestContainsHashKeyCondition(RequestWrapper request, String hashKeyField) {
        if (request.getExpression() == null) {
            // no filter criteria
            return false;
        }
        Map<String, String> expressionAttrNames = request.getExpressionAttributeNames();
        Optional<String> keyFieldName = expressionAttrNames != null ? expressionAttrNames.entrySet().stream()
            .filter(entry -> entry.getValue().equals(hashKeyField)).map(Entry::getKey).findFirst() : Optional.empty();
        String fieldToFind = (keyFieldName.orElse(hashKeyField));
        String toFind = fieldToFind + " = ";
        int start = request.getExpression().indexOf(toFind);
        return start != -1;
    }

}