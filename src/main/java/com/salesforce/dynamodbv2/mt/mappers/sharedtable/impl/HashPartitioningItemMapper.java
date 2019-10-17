/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningKeyMapper.PhysicalRangeKeyBytesConverter.fromBytes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * Maps virtual items to physical items and vice versa for shared tables using hash partitioning. More specifically,
 * <pre>
 *     physicalHashKey = tenantId + virtualTableName + hash(virtualHashKey) % numPartitions
 *     physicalRangeKey = virtualHashKey + virtualRangeKey
 * </pre>
 * All virtual field values, including those that appear in any indexes, are kept in the physical item.
 */
class HashPartitioningItemMapper implements ItemMapper {

    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;
    private final UnaryOperator<DynamoSecondaryIndex> lookUpPhysicalSecondaryIndex;
    private final HashPartitioningKeyMapper keyMapper;

    HashPartitioningItemMapper(DynamoTableDescription virtualTable,
                               DynamoTableDescription physicalTable,
                               UnaryOperator<DynamoSecondaryIndex> lookUpPhysicalSecondaryIndex,
                               HashPartitioningKeyMapper keyMapper) {
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.lookUpPhysicalSecondaryIndex = lookUpPhysicalSecondaryIndex;
        this.keyMapper = keyMapper;
    }

    @Override
    public Map<String, AttributeValue> applyForWrite(Map<String, AttributeValue> virtualItem) {
        virtualItem = Collections.unmodifiableMap(virtualItem);
        Map<String, AttributeValue> physicalItem = new HashMap<>(virtualItem);

        // copy table primary key fields
        addPhysicalKeys(virtualItem, physicalItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // copy secondary index fields
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            addPhysicalKeys(virtualItem, physicalItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), false);
        }

        return physicalItem;
    }

    @Override
    public Map<String, AttributeValue> applyToKeyAttributes(Map<String, AttributeValue> virtualItem,
                                                            @Nullable DynamoSecondaryIndex virtualSi) {
        virtualItem = Collections.unmodifiableMap(virtualItem);
        Map<String, AttributeValue> physicalItem = new HashMap<>();

        // extract table primary key fields
        addPhysicalKeys(virtualItem, physicalItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // extract secondary index fields if a secondary index is specified
        if (virtualSi != null) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            addPhysicalKeys(virtualItem, physicalItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), true);
        }

        return physicalItem;
    }

    private void addPhysicalKeys(Map<String, AttributeValue> virtualItem, Map<String, AttributeValue> physicalItem,
                                 PrimaryKey virtualPK, PrimaryKey physicalPK, boolean required) {
        ScalarAttributeType virtualHkType = virtualPK.getHashKeyType();
        AttributeValue virtualHkValue = virtualItem.get(virtualPK.getHashKey());
        if (required) {
            checkNotNull(virtualHkValue, "Item is missing hash key '" + virtualPK.getHashKey() + "'");
        }
        if (virtualPK.getRangeKey().isPresent()) {
            AttributeValue virtualRkValue = virtualItem.get(virtualPK.getRangeKey().get());
            if (required) {
                checkNotNull(virtualRkValue, "Item is missing range key '" + virtualPK.getRangeKey().get() + "'");
            }
            if (virtualHkValue != null && virtualRkValue != null) {
                // add physical HK
                physicalItem.put(physicalPK.getHashKey(), keyMapper.toPhysicalHashKey(virtualHkType, virtualHkValue));

                // add physical RK
                physicalItem.put(physicalPK.getRangeKey().get(),
                    keyMapper.toPhysicalRangeKey(virtualPK, virtualHkValue, virtualRkValue));
            }
        } else {
            if (virtualHkValue != null) {
                // add physical HK
                physicalItem.put(physicalPK.getHashKey(), keyMapper.toPhysicalHashKey(virtualHkType, virtualHkValue));

                // add physical RK
                physicalItem.put(physicalPK.getRangeKey().get(),
                    keyMapper.toPhysicalRangeKey(virtualPK, virtualHkValue));
            }
        }
    }

    @Override
    public Map<String, AttributeValue> reverse(Map<String, AttributeValue> physicalItem) {
        if (physicalItem == null) {
            return null;
        }

        Map<String, AttributeValue> virtualItem = new HashMap<>(physicalItem);

        // remove physical table PK fields and add back virtual fields if needed
        reversePhysicalKeys(virtualItem, virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), true);

        // remove physical secondary index fields and add back virtual fields if needed
        for (DynamoSecondaryIndex virtualSi : virtualTable.getSis()) {
            DynamoSecondaryIndex physicalSi = lookUpPhysicalSecondaryIndex.apply(virtualSi);
            reversePhysicalKeys(virtualItem, virtualSi.getPrimaryKey(), physicalSi.getPrimaryKey(), false);
        }

        return virtualItem;
    }

    private void reversePhysicalKeys(Map<String, AttributeValue> item, PrimaryKey virtualPK,
                                     PrimaryKey physicalPK, boolean required) {
        item.remove(physicalPK.getHashKey());
        AttributeValue physicalRkValue = item.remove(physicalPK.getRangeKey().get());
        if (required) {
            checkNotNull(physicalRkValue, "Physical item missing range key value");
        }

        // item might already have virtual PK fields populated, but not if we're reversing physical key fields only
        // (e.g., mapping a physical query's LastEvaluatedKey), so reverse physical key fields in case it's needed.
        if (physicalRkValue != null) {
            ByteBuffer physicalRkBytes = physicalRkValue.getB();
            physicalRkBytes.rewind();
            if (virtualPK.getRangeKey().isPresent()) {
                AttributeValue[] virtualValues = fromBytes(virtualPK.getHashKeyType(),
                    virtualPK.getRangeKeyType().get(), physicalRkBytes);
                item.put(virtualPK.getHashKey(), virtualValues[0]);
                item.put(virtualPK.getRangeKey().get(), virtualValues[1]);
            } else {
                item.put(virtualPK.getHashKey(), fromBytes(virtualPK.getHashKeyType(), physicalRkBytes));
            }
        }
    }

}
