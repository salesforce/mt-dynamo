/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

public class SharedTableNamingRules {

    // since table prefixes cannot have an underscore, and double underscores are not allowed in table names,
    // this should ensure dynamic physical table names do not collide with physical table names
    public static final String DYNAMIC_PHYSICAL_TABLE_PREFIX = "d__";

    public static final String VIRTUAL_MULTITENANT_TABLE_PREFIX = "mt_";

    public static void validatePhysicalTablePrefix(String tablePrefix) {
        checkArgument(tablePrefix != null, "Table prefix cannot be null");
        checkArgument(tablePrefix.matches("[0-9a-zA-Z.\\-]+"),
            "Table prefix has invalid character (allowed: [0-9 a-z A-Z . -]): %s", tablePrefix);
    }

    public static void validateStaticPhysicalTableName(String tableName) {
        validateTableName(tableName);
        checkArgument(!tableName.startsWith(DYNAMIC_PHYSICAL_TABLE_PREFIX),
            "Static physical table name cannot start with '%s'", DYNAMIC_PHYSICAL_TABLE_PREFIX);
    }

    public static void validateVirtualTableName(String tableName, boolean isMultitenant) {
        validateTableName(tableName);
        if (isMultitenant) {
            checkArgument(tableName.startsWith(VIRTUAL_MULTITENANT_TABLE_PREFIX),
                "Multitenant table name must start with '%s'", VIRTUAL_MULTITENANT_TABLE_PREFIX);
        } else {
            checkArgument(!tableName.startsWith(VIRTUAL_MULTITENANT_TABLE_PREFIX),
                "Non-multitenant table name cannot start with '%s'", VIRTUAL_MULTITENANT_TABLE_PREFIX);
        }
    }

    private static void validateTableName(String tableName) {
        checkArgument(tableName != null, "Table name cannot be null");
        checkArgument(tableName.matches("[0-9a-zA-Z_.]+"),
            "Table name has invalid character (allowed: [0-9 a-z A-Z _ .]): %s", tableName);
        checkArgument(!tableName.contains("__"), "Table name cannot contain double underscore: %s", tableName);
    }
}
