/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapperToBinary;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.HashPartitioningKeyPrefixFunction;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Shared table partitioning strategy where each virtual table has a fixed number of buckets / physical hash keys,
 * allowing us to scan efficiently. More specifically,
 * <pre>
 *     physicalHashKey = tenantId + virtualTableName + hash(virtualHashKey) % numBuckets
 *     physicalRangeKey = virtualHashKey + virtualRangeKey
 * </pre>
 * See https://salesforce.quip.com/hULMAJ0KNFUY
 */
public class HashPartitioningStrategy implements TablePartitioningStrategy {

    private final int numBucketsPerVirtualTable;

    public HashPartitioningStrategy(int numBucketsPerVirtualTable) {
        this.numBucketsPerVirtualTable = numBucketsPerVirtualTable;
    }

    @Override
    public PrimaryKeyMapper getTablePrimaryKeyMapper() {
        return new PrimaryKeyMapperToBinary();
    }

    @Override
    public PrimaryKeyMapper getSecondaryIndexPrimaryKeyMapper() {
        return new PrimaryKeyMapperToBinary();
    }

    @Override
    public boolean isPhysicalPrimaryKeyValid(PrimaryKey primaryKey) {
        return primaryKey.getHashKeyType() == B && primaryKey.getRangeKeyType().equals(Optional.of(B));
    }

    @Override
    public void validateCompatiblePrimaryKeys(PrimaryKey virtualPrimaryKey, PrimaryKey physicalPrimaryKey) {
        checkArgument(isPhysicalPrimaryKeyValid(physicalPrimaryKey),
                "physical primary key must have hash type B and range type B but is: " + physicalPrimaryKey);
    }

    @Override
    public MtContextAndTable toContextAndTable(ScalarAttributeType physicalHashKeyType,
                                               AttributeValue physicalHashKeyValue) {
        checkArgument(physicalHashKeyType == B, "Physical primary key hash type must be binary");
        return HashPartitioningKeyPrefixFunction.fromPhysicalHashKey(physicalHashKeyValue);
    }

    @Override
    public TableMapping createTableMapping(String context,
                                           DynamoTableDescription virtualTable,
                                           DynamoTableDescription physicalTable,
                                           UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper) {
        return new HashPartitioningTableMapping(context, virtualTable, physicalTable, secondaryIndexMapper,
            numBucketsPerVirtualTable);
    }
}
