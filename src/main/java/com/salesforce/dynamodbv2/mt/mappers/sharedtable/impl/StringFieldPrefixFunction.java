/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.function.Predicate;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class StringFieldPrefixFunction implements FieldPrefixFunction<String> {

    static final StringFieldPrefixFunction INSTANCE = new StringFieldPrefixFunction();

    private static final char DELIMITER = '/';

    private StringFieldPrefixFunction() {
        super();
    }

    @Override
    public String apply(FieldValue<String> fieldValue) {
        final String context = fieldValue.getContext();
        final String tableName = fieldValue.getTableName();
        final String value = fieldValue.getValue();
        // TODO turn into runtime checks?
        assert fieldValue.getContext().indexOf(DELIMITER) == -1 && fieldValue.getTableName().indexOf(DELIMITER) == -1;
        return newPrefixBuffer(context, tableName, value.length()).append(value).toString();
    }

    @Override
    public FieldValue<String> reverse(String qualifiedValue) {
        int idx = qualifiedValue.indexOf(DELIMITER);
        checkArgument(idx != -1);
        final String context = qualifiedValue.substring(0, idx);

        idx++;
        int idx2 = qualifiedValue.indexOf(DELIMITER, idx);
        checkArgument(idx2 != -1);
        final String tableName = qualifiedValue.substring(idx, idx2);

        idx2++;
        String value = qualifiedValue.substring(idx2);

        return new FieldValue<>(context, tableName, value);
    }

    @Override
    public Predicate<String> createFilter(String context, String tableName) {
        final String prefix = newPrefixBuffer(context, tableName, 0).toString();
        return qualifiedValue -> qualifiedValue.startsWith(prefix);
    }

    private StringBuilder newPrefixBuffer(String context, String tableName, int valueLength) {
        return new StringBuilder(context.length() + tableName.length() + 2 + valueLength)
            .append(context).append(DELIMITER)
            .append(tableName).append(DELIMITER);
    }

}