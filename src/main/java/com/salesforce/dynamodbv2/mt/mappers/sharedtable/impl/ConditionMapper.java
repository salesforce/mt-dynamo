package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import javax.annotation.Nullable;

/**
 * Applies mapping and prefixing to condition query and conditional update expressions.
 */
interface ConditionMapper {

    /**
     * Maps expressions in the given request for a virtual table to a request for the physical table,
     * based on the virtual table's primary key.
     */
    void apply(RequestWrapper request);

    /**
     * Maps expressions in the given request for a virtual table to a request for the physical table,
     * based on the given virtual secondary index if it's provided, or if not, on the virtual table primary key.
     */
    void apply(RequestWrapper request, @Nullable DynamoSecondaryIndex virtualSecondaryIndex);
}
