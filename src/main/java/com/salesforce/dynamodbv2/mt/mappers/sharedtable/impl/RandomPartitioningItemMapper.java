/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link ItemMapper} implementation for shared tables using random partitioning, where each physical hash key
 * corresponds to its virtual counterpart prefixed according to the given FieldMapper.
 *
 * @author msgroi
 */
class RandomPartitioningItemMapper implements ItemMapper {

    private final FieldMapper fieldMapper;
    private final List<FieldMapping> tableVirtualToPhysicalFieldMappings;
    private final Map<DynamoSecondaryIndex, List<FieldMapping>> indexVirtualToPhysicalFieldMappings;
    private final Map<String, List<FieldMapping>> allVirtualToPhysicalFieldMappings;
    private final Map<String, FieldMapping> physicalToVirtualFieldMappings;

    RandomPartitioningItemMapper(FieldMapper fieldMapper,
                                 List<FieldMapping> tablePrimaryKeyFieldMappings,
                                 Map<DynamoSecondaryIndex, List<FieldMapping>> indexPrimaryKeyFieldMappings) {
        this.fieldMapper = fieldMapper;
        this.tableVirtualToPhysicalFieldMappings = tablePrimaryKeyFieldMappings;
        this.indexVirtualToPhysicalFieldMappings = indexPrimaryKeyFieldMappings;

        // build map from each field to any PK or secondary index mappings
        this.allVirtualToPhysicalFieldMappings = new HashMap<>();
        tableVirtualToPhysicalFieldMappings.forEach(
            fieldMapping -> addFieldMapping(allVirtualToPhysicalFieldMappings, fieldMapping));
        indexVirtualToPhysicalFieldMappings.values().forEach(
            list -> list.forEach(
                fieldMapping -> addFieldMapping(allVirtualToPhysicalFieldMappings, fieldMapping)));

        // invert PK map
        this.physicalToVirtualFieldMappings = invertMapping(allVirtualToPhysicalFieldMappings);
    }

    /*
     * Helper method for adding a single FieldMapping object to the existing list of FieldMapping objects.
     */
    private static void addFieldMapping(Map<String, List<FieldMapping>> fieldMappings, FieldMapping fieldMappingToAdd) {
        String key = fieldMappingToAdd.getSource().getName();
        List<FieldMapping> fieldMapping = fieldMappings.computeIfAbsent(key, k -> new ArrayList<>());
        fieldMapping.add(fieldMappingToAdd);
    }

    @Override
    public Map<String, AttributeValue> applyForWrite(Map<String, AttributeValue> virtualItem) {
        Map<String, AttributeValue> physicalItem = new HashMap<>();
        virtualItem.forEach((field, attribute) -> {
            List<FieldMapping> fieldMappings = allVirtualToPhysicalFieldMappings.get(field);
            if (!isNullOrEmpty(fieldMappings)) {
                fieldMappings.forEach(fieldMapping -> applyFieldMapping(physicalItem, fieldMapping, attribute));
            } else {
                physicalItem.put(field, attribute);
            }
        });
        return physicalItem;
    }

    @Override
    public Map<String, AttributeValue> applyToKeyAttributes(Map<String, AttributeValue> virtualItem,
                                                            @Nullable DynamoSecondaryIndex virtualSecondaryIndex) {
        // always include table primary key
        Map<String, AttributeValue> physicalItem = new HashMap<>(
            applySpecificMappings(virtualItem, tableVirtualToPhysicalFieldMappings));
        // optionally include specified secondary index
        if (virtualSecondaryIndex != null) {
            List<FieldMapping> indexFieldMappings = indexVirtualToPhysicalFieldMappings.get(virtualSecondaryIndex);
            if (indexFieldMappings == null) {
                throw new IllegalArgumentException("No field mappings for secondary index with name "
                    + virtualSecondaryIndex.getIndexName());
            }
            physicalItem.putAll(applySpecificMappings(virtualItem, indexFieldMappings));
        }
        return physicalItem;
    }

    /**
     *  Converts a virtual item to a physical item, where we retain only the fields in the given list of field mappings.
     */
    private Map<String, AttributeValue> applySpecificMappings(Map<String, AttributeValue> virtualItem,
                                                              List<FieldMapping> fieldMappings) {
        Map<String, AttributeValue> physicalItem = new HashMap<>();
        fieldMappings.forEach(fieldMapping -> {
            String field = fieldMapping.getSource().getName();
            AttributeValue attribute = virtualItem.get(field);
            if (attribute != null) {
                applyFieldMapping(physicalItem, fieldMapping, attribute);
            } else {
                throw new IllegalArgumentException("Virtual item is missing mapping field " + field);
            }
        });
        return physicalItem;
    }

    /**
     * Applies field mapping to given value and put the result in the given item map.
     */
    private void applyFieldMapping(Map<String, AttributeValue> item, FieldMapping fieldMapping, AttributeValue value) {
        item.put(fieldMapping.getTarget().getName(),
            fieldMapping.isContextAware() ? fieldMapper.apply(fieldMapping, value) : value);
    }

    @Override
    public Map<String, AttributeValue> reverse(Map<String, AttributeValue> qualifiedItem) {
        if (qualifiedItem == null) {
            return null;
        }
        Map<String, AttributeValue> unqualifiedItem = new HashMap<>();
        qualifiedItem.forEach((field, attribute) -> {
            FieldMapping fieldMapping = physicalToVirtualFieldMappings.get(field);
            if (fieldMapping != null) {
                unqualifiedItem.put(fieldMapping.getTarget().getName(),
                    fieldMapping.isContextAware() ? fieldMapper.reverse(fieldMapping, attribute) : attribute);
            } else {
                unqualifiedItem.put(field, attribute);
            }
        });
        return unqualifiedItem;
    }

    private static Map<String, FieldMapping> invertMapping(
        Map<String, List<FieldMapping>> allVirtualToPhysicalFieldMappings) {
        Map<String, FieldMapping> fieldMappings = new HashMap<>();
        allVirtualToPhysicalFieldMappings.values().stream()
            .flatMap(Collection::stream)
            .forEach(fieldMapping -> fieldMappings.put(fieldMapping.getTarget().getName(),
                new FieldMapping(fieldMapping.getTarget(),
                    fieldMapping.getSource(),
                    fieldMapping.getVirtualIndexName(),
                    fieldMapping.getPhysicalIndexName(),
                    fieldMapping.getIndexType(),
                    fieldMapping.isContextAware())));
        return fieldMappings;
    }

}