/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * Model class for storing a mapping of a field mapping pair.
 *
 * @author msgroi
 */
class FieldMapping {
    private final Field source;
    private final Field target;
    private final String virtualIndexName;
    private final String physicalIndexName;
    private final IndexType indexType;

    private final boolean isContextAware;

    enum IndexType {
        TABLE, SECONDARYINDEX
    }

    FieldMapping(Field source,
                 Field target,
                 String virtualIndexName,
                 String physicalIndexName,
                 IndexType indexType,
                 boolean isContextAware) {
        this.source = source;
        this.target = target;
        this.virtualIndexName = virtualIndexName;
        this.physicalIndexName = physicalIndexName;

        this.indexType = indexType;
        this.isContextAware = isContextAware;
    }

    Field getSource() {
        return source;
    }

    Field getTarget() {
        return target;
    }

    String getVirtualIndexName() {
        return virtualIndexName;
    }

    String getPhysicalIndexName() {
        return physicalIndexName;
    }

    IndexType getIndexType() {
        return indexType;
    }

    boolean isContextAware() {
        return isContextAware;
    }

    static class Field {

        private final String name;
        private final ScalarAttributeType type;

        Field(String name, ScalarAttributeType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public ScalarAttributeType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Field field = (Field) o;

            return name.equals(field.name)
                && type == field.type;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldMapping that = (FieldMapping) o;

        return isContextAware == that.isContextAware
            && source.equals(that.source)
            && target.equals(that.target)
            && virtualIndexName.equals(that.virtualIndexName)
            && physicalIndexName.equals(that.physicalIndexName)
            && indexType == that.indexType;
    }
}
