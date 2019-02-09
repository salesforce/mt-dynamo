package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

public interface TableMapping {

    DynamoTableDescription getVirtualTable();

    DynamoTableDescription getPhysicalTable();

    ItemMapper getItemMapper();

    QueryAndScanMapper getQueryAndScanMapper();

    ConditionMapper getConditionMapper();
}
