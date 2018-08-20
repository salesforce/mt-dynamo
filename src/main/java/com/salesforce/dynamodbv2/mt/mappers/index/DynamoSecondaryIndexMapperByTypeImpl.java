/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;

import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Finds the physical index matching the virtual provided index by matching HASH and RANGE keys by data type.
 *
 * @author msgroi
 */
public class DynamoSecondaryIndexMapperByTypeImpl implements DynamoSecondaryIndexMapper {

    private static final PrimaryKeyMapper PRIMARY_KEY_MAPPER = new PrimaryKeyMapperByTypeImpl(true);

    @Override
    public DynamoSecondaryIndex lookupPhysicalSecondaryIndex(DynamoSecondaryIndex virtualSi,
                                                             DynamoTableDescription physicalTable)
        throws MappingException {
        return (DynamoSecondaryIndex) PRIMARY_KEY_MAPPER.mapPrimaryKey(virtualSi.getPrimaryKey(),
            physicalTable.getSis().stream()
                .filter(dynamoSecondaryIndex -> dynamoSecondaryIndex.getType() == virtualSi.getType())
                .map((Function<DynamoSecondaryIndex, HasPrimaryKey>) dynamoSecondaryIndex -> dynamoSecondaryIndex)
                .collect(Collectors.toList()));
    }

}