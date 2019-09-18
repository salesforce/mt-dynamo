/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import javax.annotation.Nullable;

/**
 * Applies mapping and prefixing to condition query and conditional update expressions.
 */
interface ConditionMapper {

    /**
     * Maps the update expression, condition expression, and expression attributes in the given virtual update request
     * to the corresponding physical update request.
     */
    void applyForUpdate(RequestWrapper request);

    /**
     * Maps the key condition in the given virtual query request to the corresponding physical key condition,
     * based on the given virtual secondary index if it's provided, or if not, on the virtual table primary key.
     */
    void applyToKeyCondition(RequestWrapper request, @Nullable DynamoSecondaryIndex virtualSecondaryIndex);

    /**
     * Maps the filter expression in the given virtual request to the corresponding physical filter expression,
     * applying the change to either the primary or secondary expression in the request wrapper.
     */
    void applyToFilterExpression(RequestWrapper request, boolean isPrimaryExpression);

}
