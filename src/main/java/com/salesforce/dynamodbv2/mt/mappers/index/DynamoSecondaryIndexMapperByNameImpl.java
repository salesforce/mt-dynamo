/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

/**
 * Finds the physical index matching the virtual provided index by matching by index name.
 *
 * @author msgroi
 */
public class DynamoSecondaryIndexMapperByNameImpl implements DynamoSecondaryIndexMapper {

    @Override
    public DynamoSecondaryIndex lookupPhysicalSecondaryIndex(DynamoSecondaryIndex virtualSi,
                                                             DynamoTableDescription physicalTable) {
        return physicalTable.findSi(virtualSi.getIndexName());
    }

}