/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.google.common.base.Splitter;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class FieldPrefixFunction {

    private final String delimiter;

    FieldPrefixFunction(String delimiter) {
        this.delimiter = delimiter;
    }

    FieldValue apply(MtAmazonDynamoDbContextProvider mtContext, String tableIndex, String value) {
        return new FieldValue(mtContext.getContext(),
            tableIndex,
            mtContext.getContext() + delimiter + tableIndex + delimiter + value,
            value);
    }

    FieldValue reverse(String qualifiedValue) {
        int prefixSeparatorIndex = StringUtils.ordinalIndexOf(qualifiedValue, delimiter, 2);
        List<String> prefixList = Splitter.on(delimiter).splitToList(qualifiedValue.substring(0, prefixSeparatorIndex));
        return new FieldValue(prefixList.get(0),
            prefixList.get(1),
            qualifiedValue,
            qualifiedValue.substring(prefixSeparatorIndex + 1));

    }

    static class FieldValue {
        private final String mtContext;
        private final String tableIndex;
        private final String qualifiedValue;
        private final String unqualifiedValue;

        FieldValue(String mtContext, String tableIndex, String qualifiedValue, String unqualifiedValue) {
            this.mtContext = mtContext;
            this.tableIndex = tableIndex;
            this.qualifiedValue = qualifiedValue;
            this.unqualifiedValue = unqualifiedValue;
        }

        String getMtContext() {
            return mtContext;
        }

        String getTableIndex() {
            return tableIndex;
        }

        String getQualifiedValue() {
            return qualifiedValue;
        }

        String getUnqualifiedValue() {
            return unqualifiedValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldValue that = (FieldValue) o;

            return mtContext.equals(that.mtContext)
                    && tableIndex.equals(that.tableIndex)
                    && qualifiedValue.equals(that.qualifiedValue)
                    && unqualifiedValue.equals(that.unqualifiedValue);
        }
    }

}
