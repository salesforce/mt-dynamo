/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
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

    private static final char DELIMITER = '.';

    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;
    private final ItemMapper itemMapper;

    public HashPartitioningTableMapping(DynamoTableDescription virtualTable,
                                        DynamoTableDescription physicalTable,
                                        UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                        MtAmazonDynamoDbContextProvider mtContext,
                                        int numBucketsPerVirtualHashKey) {
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.itemMapper = new HashPartitioningItemMapper(virtualTable, physicalTable, secondaryIndexMapper, mtContext,
            numBucketsPerVirtualHashKey, DELIMITER);
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
    public ItemMapper getItemMapper() {
        return itemMapper;
    }

    @Override
    public RecordMapper getRecordMapper() {
        // TODO
        return null;
    }

    @Override
    public QueryAndScanMapper getQueryAndScanMapper() {
        // TODO
        return null;
    }

    @Override
    public ConditionMapper getConditionMapper() {
        // TODO implement ConditionMapper (returning something non-null for now so we can run PutTest)
        return new ConditionMapper() {

            @Override
            public void applyForUpdate(RequestWrapper request) {

            }

            @Override
            public void applyToKeyCondition(RequestWrapper request, @Nullable DynamoSecondaryIndex virtualSecondaryIndex) {

            }

            @Override
            public void applyToFilterExpression(RequestWrapper request, boolean isPrimaryExpression) {

            }

        };
    }

    @Override
    public String toString() {
        return String.format("%s -> %s, virtual: %s, physical: %s",
            getVirtualTable().getTableName(), getPhysicalTable().getTableName(),
            getVirtualTable().toString(), getPhysicalTable().toString());
    }
}
