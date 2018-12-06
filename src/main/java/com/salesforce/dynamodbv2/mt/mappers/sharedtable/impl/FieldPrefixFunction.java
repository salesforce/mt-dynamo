/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.util.CompositeStrings;

import java.util.List;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class FieldPrefixFunction {

    private final CompositeStrings compositeStrings;

    FieldPrefixFunction(char delimiter) {
        compositeStrings = new CompositeStrings(delimiter, '\\');
    }

    FieldValue apply(MtAmazonDynamoDbContextProvider mtContext, String tableIndex, String value) {
        return new FieldValue(mtContext.getContext(),
            tableIndex,
            compositeStrings.join(ImmutableList.of(mtContext.getContext(), tableIndex, value)),
            value);
    }

    FieldValue reverse(String qualifiedValue) {
        List<String> prefixList = Lists.newArrayList(compositeStrings.split(qualifiedValue));
        return new FieldValue(prefixList.get(0),
            prefixList.get(1),
            qualifiedValue,
            prefixList.get(2));
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

        @Override
        public String toString() {
            return "FieldValue{"
                    + "mtContext='" + mtContext + '\''
                    + ", tableIndex='" + tableIndex + '\''
                    + ", qualifiedValue='" + qualifiedValue + '\''
                    + ", unqualifiedValue='" + unqualifiedValue + '\''
                    + '}';
        }

    }

}