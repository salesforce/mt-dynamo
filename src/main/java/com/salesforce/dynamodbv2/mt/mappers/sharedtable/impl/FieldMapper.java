/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.nio.charset.StandardCharsets.UTF_8;

/*
 * Adds and removes prefixes to fields based on the tenant context.  If isPolymorphicTable is set to true, also includes
 * the virtual table name in the prefix.
 *
 * @author msgroi
 */
class FieldMapper {

    private final MTAmazonDynamoDBContextProvider mtContext;
    private final String delimiter;
    private final String virtualTableName;
    private final boolean isPolymorphicTable;

    FieldMapper(MTAmazonDynamoDBContextProvider mtContext,
                String delimiter,
                String virtualTableName,
                boolean isPolymorphicTable) {
        this.mtContext = mtContext;
        this.delimiter = delimiter;
        this.virtualTableName = virtualTableName;
        this.isPolymorphicTable = isPolymorphicTable;
    }

    AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
        return new AttributeValue(getPrefix(fieldMapping) + convertToStringNotNull(fieldMapping.getSource().getType(), unqualifiedAttribute));
    }

    AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
        String qualifiedValue = qualifiedAttribute.getS();
        String expectedPrefix = getPrefix(fieldMapping);
        String actualPrefix = qualifiedValue.substring(0, expectedPrefix.length());
        checkArgument(actualPrefix.equalsIgnoreCase(expectedPrefix),
                "qualified value expected to start with '" + expectedPrefix + "' but started with '" + actualPrefix + "'");
        return convertFromString(fieldMapping.getTarget().getType(), qualifiedValue.substring(expectedPrefix.length()));
    }

    private String getPrefix(FieldMapping fieldMapping) {
        StringBuilder sb = new StringBuilder();
        sb.append(mtContext.getContext()).append(delimiter);
        if (fieldMapping.getIndexType() == TABLE) {
            if (isPolymorphicTable) {
                sb.append(virtualTableName).append(delimiter);
            }
        } else {
            // SECONDARYINDEX
            sb.append(fieldMapping.getVirtualIndexName()).append(delimiter);
        }
        return sb.toString();
    }

    private String convertToStringNotNull(ScalarAttributeType type, AttributeValue attributeValue) {
        String convertedString = convertToString(type, attributeValue);
        checkNotNull(convertedString, "attributeValue=" + attributeValue + " of type=" + type.name() + " could not be converted");
        return convertedString;
    }

    private String convertToString(ScalarAttributeType type, AttributeValue attributeValue) {
        checkNotNull(type, "null attribute type");
        switch (type) {
            case S:
                return attributeValue.getS();
            case N:
                return attributeValue.getN();
            case B:
                return UTF_8.decode(attributeValue.getB()).toString();
            default:
                throw new IllegalArgumentException("unexpected type " + type + " encountered");
        }
    }

    private AttributeValue convertFromString(ScalarAttributeType type, String value) {
        AttributeValue unqualifiedAttribute = new AttributeValue();
        switch (type) {
            case S:
                return unqualifiedAttribute.withS(value);
            case N:
                return unqualifiedAttribute.withN(value);
            case B:
                return unqualifiedAttribute.withB(UTF_8.encode(value));
            default:
                throw new IllegalArgumentException("unexpected type " + type + " encountered");
        }
    }

}