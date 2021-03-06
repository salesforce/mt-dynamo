/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

/**
 * Applies mapping and prefixing to condition query and conditional update expressions.
 */
interface ConditionMapper {

    /**
     * Maps the update expression, condition expression, and expression attributes in the given virtual update request
     * to the corresponding physical update request.
     */
    void applyForUpdate(UpdateItemRequest updateItemRequest);

    /**
     * Maps the key condition in the given virtual query request to the corresponding physical key condition,
     * based on the given virtual secondary index if it's provided, or if not, on the virtual table primary key.
     */
    void applyToKeyCondition(RequestWrapper request, RequestIndex requestIndex, String filterExpression);

    /**
     * Maps the filter expression in the given virtual request to the corresponding physical filter expression,
     * applying the change to either the primary or secondary expression in the request wrapper.
     *
     * <p>Used to map filter/condition expressions in a put/delete/scan/query request, which in terms of RequestWrapper
     * is primary in the case of put/delete/scan, and secondary in a query request.
     */
    void applyToFilterExpression(RequestWrapper request);

}
