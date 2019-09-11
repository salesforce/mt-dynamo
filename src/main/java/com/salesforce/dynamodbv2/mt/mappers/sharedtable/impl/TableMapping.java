package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

/**
 * Holds the state of mapping of a virtual table to a physical table. Provides methods for retrieving the virtual
 * and physical descriptions, and the mapping of fields from virtual to physical and back.
 */
public interface ITableMapping {

    DynamoTableDescription getVirtualTable();

    DynamoTableDescription getPhysicalTable();

    IItemMapper getItemMapper();

    IRecordMapper getRecordMapper();

    IQueryAndScanMapper getQueryAndScanMapper();

    IConditionMapper getConditionMapper();
}
