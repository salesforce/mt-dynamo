/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Map;
import java.util.function.Function;

class HashPartitioningQueryAndScanMapper extends AbstractQueryAndScanMapper {

    private final DynamoTableDescription physicalTable;
    private final HashPartitioningKeyMapper keyMapper;
    private final int maxBucketNumber;

    HashPartitioningQueryAndScanMapper(String context,
                                       DynamoTableDescription physicalTable,
                                       Function<String, RequestIndex> indexLookup,
                                       HashPartitioningConditionMapper conditionMapper,
                                       HashPartitioningItemMapper itemMapper,
                                       HashPartitioningKeyMapper keyMapper) {
        super(context, indexLookup, conditionMapper, itemMapper);
        this.physicalTable = physicalTable;
        this.keyMapper = keyMapper;
        this.maxBucketNumber = keyMapper.getNumberOfBucketsPerVirtualTable() - 1;
    }

    @Override
    ScanResult executeScanInternal(AmazonDynamoDB amazonDynamoDB, ScanRequest scanRequest) {
        RequestIndex requestIndex = getRequestIndexAndMapIndexName(new ScanRequestWrapper(scanRequest, null, null));
        PrimaryKey virtualPk = requestIndex.getVirtualPk();
        PrimaryKey physicalPk = requestIndex.getPhysicalPk();

        QueryRequest queryRequest = new QueryRequest()
            .withTableName(physicalTable.getTableName())
            .withIndexName(scanRequest.getIndexName())                  // already mapped index name
            .withExclusiveStartKey(scanRequest.getExclusiveStartKey())  // map later for easy access of virtual HK value
            .withFilterExpression(scanRequest.getFilterExpression())
            .withProjectionExpression(scanRequest.getProjectionExpression())
            .withExpressionAttributeNames(scanRequest.getExpressionAttributeNames())
            .withExpressionAttributeValues(scanRequest.getExpressionAttributeValues())
            .withReturnConsumedCapacity(scanRequest.getReturnConsumedCapacity())
            .withConsistentRead(scanRequest.getConsistentRead())
            .withLimit(scanRequest.getLimit());
        QueryRequestWrapper queryRequestWrapper = new QueryRequestWrapper(queryRequest, null, null);

        // see which bucket we should start at. if there's an ExclusiveStartKey, then use the corresponding bucket.
        // otherwise, start from the beginning at 0.
        int bucket = 0;
        if (scanRequest.getExclusiveStartKey() != null) {
            AttributeValue virtualHkValue = scanRequest.getExclusiveStartKey().get(virtualPk.getHashKey());
            bucket = keyMapper.getBucketNumber(virtualPk.getHashKeyType(), virtualHkValue);
            // map to physical start key
            applyExclusiveStartKey(queryRequestWrapper, requestIndex);
        }
        // build initial physical query request, where the key condition specifies the physical HK that corresponds to
        // the initial bucket.
        String physicalHkFieldPlaceholder = MappingUtils.getNextFieldPlaceholder(queryRequestWrapper);
        String physicalHkValuePlaceholder = MappingUtils.getNextValuePlaceholder(queryRequestWrapper);
        queryRequest.withKeyConditionExpression(physicalHkFieldPlaceholder + " = " + physicalHkValuePlaceholder)
            .addExpressionAttributeNamesEntry(physicalHkFieldPlaceholder, physicalPk.getHashKey())
            .addExpressionAttributeValuesEntry(physicalHkValuePlaceholder, keyMapper.toPhysicalHashKey(bucket));

        // keep moving forward pages until we find at least one record or reach end
        QueryResult queryResult;
        while ((queryResult = amazonDynamoDB.query(queryRequest)).getItems().isEmpty()) {
            checkState(queryResult.getLastEvaluatedKey() == null,
                "Query without filter expression should not return no items while having a lastEvaluatedKey");

            // if we're here, then this bucket has no records. go on to next bucket, unless we're already at the last
            if (bucket >= maxBucketNumber) {
                break;
            }
            bucket++;
            queryRequest.getExpressionAttributeValues().put(physicalHkValuePlaceholder,
                keyMapper.toPhysicalHashKey(bucket));
            queryRequest.withExclusiveStartKey(null);
        }
        // TODO aggregate scannedCount & consumedCapacity if returnConsumedCapacity == true?

        // if we got some records from the current bucket, and the LastEvaluatedKey is null (i.e., no more records in
        // this bucket), but we're not at the last bucket yet, then it's possible that there are more records in
        // subsequent buckets. in that case, indicate we haven't reached the end by setting the LastEvaluatedKey.
        Map<String, AttributeValue> lastEvaluatedKey = queryResult.getLastEvaluatedKey();
        if (!queryResult.getItems().isEmpty() && lastEvaluatedKey == null && bucket < maxBucketNumber) {
            lastEvaluatedKey = queryResult.getItems().get(queryResult.getItems().size() - 1);
        }

        // convert to a ScanResult
        return new ScanResult()
            .withItems(queryResult.getItems())
            .withLastEvaluatedKey(lastEvaluatedKey)
            .withCount(queryResult.getCount());
    }

}
