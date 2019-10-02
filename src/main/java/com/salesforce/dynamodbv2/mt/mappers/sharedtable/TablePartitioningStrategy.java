/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapperToBinary;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTableMapping;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RandomPartitioningTableMapping;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMapping;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Defines how we partition data in shared tables. The supported strategies are random partitioning and hash
 * partitioning. See https://salesforce.quip.com/hULMAJ0KNFUY.
 */
public interface TablePartitioningStrategy {

    /**
     * How we map a virtual table to a physical table based on primary key field types.
     */
    PrimaryKeyMapper getTablePrimaryKeyMapper();

    /**
     * How we map virtual secondary indexes to physical secondary indexes based on primary key field types.
     */
    PrimaryKeyMapper getSecondaryIndexPrimaryKeyMapper();

    /**
     * Returns whether a given physical table or index primary key is valid.
     */
    boolean isPhysicalPrimaryKeyValid(PrimaryKey primaryKey);

    /**
     * Validates that the given virtual and physical primary keys are compatible.
     */
    void validateCompatiblePrimaryKeys(PrimaryKey virtualPrimaryKey, PrimaryKey physicalPrimaryKey);

    TableMapping createTableMapping(DynamoTableDescription virtualTable,
                                    DynamoTableDescription physicalTable,
                                    UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                    MtAmazonDynamoDbContextProvider mtContext);

    class RandomPartitioningStrategy implements TablePartitioningStrategy {

        @Override
        public PrimaryKeyMapper getTablePrimaryKeyMapper() {
            return new PrimaryKeyMapperByTypeImpl(false);
        }

        @Override
        public PrimaryKeyMapper getSecondaryIndexPrimaryKeyMapper() {
            return new PrimaryKeyMapperByTypeImpl(true);
        }

        @Override
        public boolean isPhysicalPrimaryKeyValid(PrimaryKey primaryKey) {
            return primaryKey.getHashKeyType() == S || primaryKey.getHashKeyType() == B;
        }

        @Override
        public void validateCompatiblePrimaryKeys(PrimaryKey virtualPrimaryKey, PrimaryKey physicalPrimaryKey) {
            checkNotNull(virtualPrimaryKey.getHashKey(), "hash key is required on virtual table");
            checkNotNull(physicalPrimaryKey.getHashKey(), "hash key is required on physical table");
            checkArgument(physicalPrimaryKey.getHashKeyType() == S || physicalPrimaryKey.getHashKeyType() == B,
                    "physical hash key must be of type S or B");
            if (virtualPrimaryKey.getRangeKey().isPresent()) {
                checkArgument(physicalPrimaryKey.getRangeKey().isPresent(),
                        "rangeKey exists on virtual primary key but not on physical");
                checkArgument(virtualPrimaryKey.getRangeKeyType().orElseThrow()
                                == physicalPrimaryKey.getRangeKeyType().orElseThrow(),
                        "virtual and physical range-key types mismatch");
            }
        }

        @Override
        public TableMapping createTableMapping(DynamoTableDescription virtualTable,
                                               DynamoTableDescription physicalTable,
                                               UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                               MtAmazonDynamoDbContextProvider mtContext) {
            return new RandomPartitioningTableMapping(virtualTable, physicalTable, secondaryIndexMapper, mtContext);
        }
    }

    class HashPartitioningStrategy implements TablePartitioningStrategy {

        private final int numBucketsPerVirtualHashKey;

        public HashPartitioningStrategy(int numBucketsPerVirtualHashKey) {
            this.numBucketsPerVirtualHashKey = numBucketsPerVirtualHashKey;
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
        public TableMapping createTableMapping(DynamoTableDescription virtualTable,
                                               DynamoTableDescription physicalTable,
                                               UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                               MtAmazonDynamoDbContextProvider mtContext) {
            return new HashPartitioningTableMapping(virtualTable, physicalTable, secondaryIndexMapper, mtContext,
                numBucketsPerVirtualHashKey);
        }
    }
}
