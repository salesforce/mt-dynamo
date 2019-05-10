/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class StringFieldPrefixFunction implements FieldPrefixFunction<String> {

    private static final char DELIMITER = '/';

    StringFieldPrefixFunction() {
    }

    @Override
    public String apply(FieldValue<String> fieldValue) {
        // TODO turn into runtime checks?
        assert fieldValue.getContext().indexOf(DELIMITER) == -1 && fieldValue.getTableName().indexOf(DELIMITER) == -1;
        return fieldValue.getContext() + DELIMITER + fieldValue.getTableName() + DELIMITER + fieldValue.getValue();
    }

    @Override
    public FieldValue reverse(String qualifiedValue) {
        int idx = qualifiedValue.indexOf(DELIMITER);
        checkArgument(idx != -1);
        String context = qualifiedValue.substring(0, idx);

        idx++;
        int idx2 = qualifiedValue.indexOf(DELIMITER, idx);
        checkArgument(idx2 != -1);
        String tableName = qualifiedValue.substring(idx, idx2);

        idx2++;
        String value = qualifiedValue.substring(idx2);

        return new FieldValue<>(context, tableName, value);
    }

}