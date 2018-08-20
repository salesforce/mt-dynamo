/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

/**
 * Allows customization of mapping of virtual to physical secondary indexes.
 *
 * @author msgroi
 */
public interface DynamoSecondaryIndexMapper {

    /*
     * Takes a virtual secondary index and the physical secondary index and returns the appropriate physical
     * secondary index.
     */
    DynamoSecondaryIndex lookupPhysicalSecondaryIndex(DynamoSecondaryIndex virtualSi,
                                                      DynamoTableDescription physicalTable) throws MappingException;

}