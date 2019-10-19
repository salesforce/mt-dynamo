/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtContextAndTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMapping;
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

    /**
     * Parses a given physical hash key value into the record's MT context and virtual table name.
     */
    MtContextAndTable toContextAndTable(ScalarAttributeType physicalHashKeyType, AttributeValue physicalHashKeyValue);

    TableMapping createTableMapping(DynamoTableDescription virtualTable,
                                    DynamoTableDescription physicalTable,
                                    UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                    MtAmazonDynamoDbContextProvider mtContext);

}
