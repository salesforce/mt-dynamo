/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps items representing records in virtual tables so they can be read from and written to their physical table
 * counterpart according to the provided TableMapping, delegating field prefixing to the provided FieldMapper.
 * The apply() method is used to map keys in getItem, putItem, updateItem, deleteItem and for mapping item responses
 * in query and scan.
 *
 * @author msgroi
 */
class ItemMapper {

    private final FieldMapper fieldMapper;
    private final TableMapping tableMapping;

    ItemMapper(TableMapping tableMapping, FieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.tableMapping = tableMapping;
    }

    /*
     * Takes a map representing a record in a virtual table that is effectively unqualified with respect to multitenant
     * context and returns a map representing a record in the physical table that is qualified with multitenant
     * context appropriately.
     *
     * Used for adding context to GetItemRequest, PutItemRequest, UpdateItemRequest, or DeleteItemRequest objects.
     */
    Map<String, AttributeValue> apply(Map<String, AttributeValue> unqualifiedItem) {
        Map<String, AttributeValue> qualifiedItem = new HashMap<>();
        Map<String, List<FieldMapping>> virtualToPhysicalFieldMappings =
            tableMapping.getAllVirtualToPhysicalFieldMappings();
        unqualifiedItem.forEach((field, attribute) -> {
            List<FieldMapping> fieldMappings = virtualToPhysicalFieldMappings.get(field);
            if (!isNullOrEmpty(fieldMappings)) {
                fieldMappings.forEach(fieldMapping -> qualifiedItem.put(fieldMapping.getTarget().getName(),
                    fieldMapping.isContextAware()
                        ? fieldMapper.apply(fieldMapping, attribute)
                        : attribute));
            } else {
                qualifiedItem.put(field, attribute);
            }
        });

        return qualifiedItem;
    }

    /*
     * Takes a map representing a record in a physical table that is effectively qualified with multitenant context and
     * returns a map representing a record in the virtual table with qualifications removed.
     *
     * Used for removing context from GetItemResult, QueryResult, or ScanResult objects.
     */
    Map<String, AttributeValue> reverse(Map<String, AttributeValue> qualifiedItem) {
        if (qualifiedItem == null) {
            return null;
        }
        Map<String, AttributeValue> unqualifiedItem = new HashMap<>();
        Map<String, List<FieldMapping>> physicalToVirtualFieldMappings =
            tableMapping.getAllPhysicalToVirtualFieldMappings();
        qualifiedItem.forEach((field, attribute) -> {
            List<FieldMapping> fieldMappings = physicalToVirtualFieldMappings.get((field));
            if (fieldMappings != null && !fieldMappings.isEmpty()) {
                fieldMappings.forEach(fieldMapping -> unqualifiedItem.put(fieldMapping.getTarget().getName(),
                    fieldMapping.isContextAware()
                        ? fieldMapper.reverse(fieldMapping, attribute)
                        : attribute));
            } else {
                unqualifiedItem.put(field, attribute);
            }
        });
        return unqualifiedItem;
    }

}