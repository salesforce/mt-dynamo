/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.HashPartitioningKeyPrefixFunction;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * {@link TableMapping} implementation for shared tables using hash partitioning, where
 * <pre>
 *     hashKey = tenantId + virtualTableName + hash(virtualHashKey) % numPartitions
 *     rangeKey = virtualHashKey + virtualRangeKey
 * </pre>
 * See https://salesforce.quip.com/hULMAJ0KNFUY
 */
public class HashPartitioningTableMapping implements TableMapping {

    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;
    private final UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper;
    private final HashPartitioningItemMapper itemMapper;
    private final HashPartitioningConditionMapper conditionMapper;
    private final HashPartitioningQueryAndScanMapper queryAndScanMapper;
    private final RecordMapper recordMapper;

    public HashPartitioningTableMapping(String context,
                                        DynamoTableDescription virtualTable,
                                        DynamoTableDescription physicalTable,
                                        UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                        int numBucketsPerVirtualTable) {
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.secondaryIndexMapper = secondaryIndexMapper;
        HashPartitioningKeyMapper keyMapper = new HashPartitioningKeyMapper(context, virtualTable.getTableName(),
            numBucketsPerVirtualTable);
        this.itemMapper = new HashPartitioningItemMapper(virtualTable, physicalTable, secondaryIndexMapper, keyMapper);
        this.conditionMapper = new HashPartitioningConditionMapper(context, virtualTable, itemMapper, keyMapper);
        this.queryAndScanMapper = new HashPartitioningQueryAndScanMapper(context, physicalTable, this::getRequestIndex,
            conditionMapper, itemMapper, keyMapper);
        this.recordMapper = new RecordMapper(context, virtualTable.getTableName(),
            physicalTable.getPrimaryKey().getHashKey(),
            v -> isMatchingPhysicalHashKey(context, virtualTable.getTableName(), v),
            itemMapper);
    }

    @Override
    public DynamoTableDescription getVirtualTable() {
        return virtualTable;
    }

    @Override
    public DynamoTableDescription getPhysicalTable() {
        return physicalTable;
    }

    @Override
    public HashPartitioningItemMapper getItemMapper() {
        return itemMapper;
    }

    @Override
    public RecordMapper getRecordMapper() {
        return recordMapper;
    }

    @Override
    public HashPartitioningQueryAndScanMapper getQueryAndScanMapper() {
        return queryAndScanMapper;
    }

    @Override
    public HashPartitioningConditionMapper getConditionMapper() {
        return conditionMapper;
    }

    @Override
    public RequestIndex getRequestIndex(@Nullable String virtualSecondaryIndexName) {
        return RequestIndex.fromVirtualSecondaryIndexName(virtualTable, physicalTable, secondaryIndexMapper,
            virtualSecondaryIndexName);
    }

    @Override
    public String toString() {
        return String.format("%s -> %s, virtual: %s, physical: %s",
            getVirtualTable().getTableName(), getPhysicalTable().getTableName(),
            getVirtualTable().toString(), getPhysicalTable().toString());
    }

    private static boolean isMatchingPhysicalHashKey(String context,
                                                     String virtualTableName,
                                                     AttributeValue value) {
        MtContextAndTable mtContextAndTable = HashPartitioningKeyPrefixFunction.fromPhysicalHashKey(value);
        return mtContextAndTable.getContext().equals(context)
            && mtContextAndTable.getTableName().equals(virtualTableName);
    }
}
