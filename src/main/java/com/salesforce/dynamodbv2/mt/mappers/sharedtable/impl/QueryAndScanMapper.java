/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

/**
 * Maps query and scan requests against virtual tables to their physical table counterparts.
 */
interface QueryAndScanMapper {

    /*
     * Takes a QueryRequest representing a query against a virtual table and mutates it so it can be applied to its
     * physical table counterpart.
     */
    void apply(QueryRequest queryRequest);

    /*
     * Takes a ScanRequest representing a scan against a virtual table and mutates it so it can be applied to its
     * physical table counterpart.
     */
    void apply(ScanRequest scanRequest);
}
