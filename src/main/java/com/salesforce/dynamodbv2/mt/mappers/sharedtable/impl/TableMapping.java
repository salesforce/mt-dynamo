/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

/**
 * Holds the state of mapping of a virtual table to a physical table. Provides methods for retrieving the virtual
 * and physical descriptions, and the mapping of fields from virtual to physical and back.
 */
public interface TableMapping {

    DynamoTableDescription getVirtualTable();

    DynamoTableDescription getPhysicalTable();

    ItemMapper getItemMapper();

    RecordMapper getRecordMapper();

    QueryAndScanMapper getQueryAndScanMapper();

    ConditionMapper getConditionMapper();
}
