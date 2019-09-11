package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

/**
 * Maps query and scan requests against virtual tables to their physical table counterparts.
 */
interface IQueryAndScanMapper {

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
