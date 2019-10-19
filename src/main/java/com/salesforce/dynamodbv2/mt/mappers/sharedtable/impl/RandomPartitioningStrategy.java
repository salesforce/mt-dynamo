/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.TablePartitioningStrategy;
import java.util.function.UnaryOperator;

/**
 * Shared table strategy where records are randomly partitioned based on a context-prefixed hash key. That is, where
 * <pre>
 *     physicalHashKey = tenantId + virtualTableName + virtualHashKey
 *     physicalRangeKey = virtualRangeKey
 * </pre>
 * See https://salesforce.quip.com/hULMAJ0KNFUY
 */
public class RandomPartitioningStrategy implements TablePartitioningStrategy {

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
    public MtContextAndTable toContextAndTable(ScalarAttributeType physicalHashKeyType,
                                               AttributeValue physicalHashKeyValue) {
        switch (physicalHashKeyType) {
            case S:
                return StringFieldPrefixFunction.INSTANCE.reverse(physicalHashKeyValue.getS());
            case B:
                return BinaryFieldPrefixFunction.INSTANCE.reverse(physicalHashKeyValue.getB());
            default:
                throw new IllegalStateException("Unsupported physical table hash key type " + physicalHashKeyType);
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
