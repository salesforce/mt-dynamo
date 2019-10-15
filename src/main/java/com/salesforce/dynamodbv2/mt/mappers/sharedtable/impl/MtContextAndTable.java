/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

/**
 * Represents a record's MT context and virtual table name.
 */
public class MtContextAndTable {

    private final String context;
    private final String tableName;

    MtContextAndTable(String context, String tableName) {
        this.context = context;
        this.tableName = tableName;
    }

    String getContext() {
        return context;
    }

    String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MtContextAndTable that = (MtContextAndTable) o;
        return context.equals(that.context)
            && tableName.equals(that.tableName);
    }

    @Override
    public String toString() {
        return "{context='" + context + "', tableName='" + tableName + "'}";
    }
}
