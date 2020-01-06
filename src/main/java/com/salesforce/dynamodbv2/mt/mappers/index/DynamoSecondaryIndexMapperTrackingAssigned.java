/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A secondary index mapper that keeps track of what the previous virtual-to-physical index assignments were, so that
 * assigned physical indexes are not reused.
 */
public class DynamoSecondaryIndexMapperTrackingAssigned implements DynamoSecondaryIndexMapper {

    private final PrimaryKeyMapper primaryKeyMapper;
    private final Map<DynamoSecondaryIndex, DynamoSecondaryIndex> assignedVirtualToPhysicalIndexes;

    public DynamoSecondaryIndexMapperTrackingAssigned(PrimaryKeyMapper primaryKeyMapper) {
        this.primaryKeyMapper = primaryKeyMapper;
        this.assignedVirtualToPhysicalIndexes = new HashMap<>();
    }

    @Override
    public DynamoSecondaryIndex lookupPhysicalSecondaryIndex(DynamoSecondaryIndex virtualSi,
                                                             DynamoTableDescription physicalTable)
        throws MappingException {
        // if the given virtual index already has an assigned physical index, return that
        DynamoSecondaryIndex physicalSi = assignedVirtualToPhysicalIndexes.get(virtualSi);

        if (physicalSi == null) {
            // if there isn't an assignment yet, look amongst the remaining physical indexes for one that is compatible,
            // according to our PrimaryKeyMapper
            Collection<DynamoSecondaryIndex> assignedPhysicalSis = assignedVirtualToPhysicalIndexes.values();
            List<HasPrimaryKey> unassignedPhysicalSis = physicalTable.getSis().stream()
                .filter(index -> index.getType() == virtualSi.getType() && !assignedPhysicalSis.contains(index))
                .collect(Collectors.toList());
            physicalSi = (DynamoSecondaryIndex) primaryKeyMapper.mapPrimaryKey(virtualSi.getPrimaryKey(),
                unassignedPhysicalSis);
            // if we got here, then we found a compatible physical index to return. add the assignment to the map.
            assignedVirtualToPhysicalIndexes.put(virtualSi, physicalSi);
        }
        return physicalSi;
    }

    public Map<DynamoSecondaryIndex, DynamoSecondaryIndex> getAssignedVirtualToPhysicalIndexes() {
        return assignedVirtualToPhysicalIndexes;
    }
}
