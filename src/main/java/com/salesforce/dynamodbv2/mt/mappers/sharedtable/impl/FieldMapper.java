/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;

/**
 * Adds and removes prefixes to fields based on the tenant context.
 *
 * @author msgroi
 */
class FieldMapper {

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final String virtualTableName;
    private final FieldPrefixFunction fieldPrefixFunction;

    FieldMapper(MtAmazonDynamoDbContextProvider mtContext,
                String virtualTableName,
                FieldPrefixFunction fieldPrefixFunction) {
        this.mtContext = mtContext;
        this.virtualTableName = virtualTableName;
        this.fieldPrefixFunction = fieldPrefixFunction;
    }

    AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
        return new AttributeValue(
            fieldPrefixFunction.apply(mtContext,
                fieldMapping.getIndexType() == TABLE
                    ? virtualTableName
                    : fieldMapping.getVirtualIndexName(),
                convertToStringNotNull(fieldMapping.getSource().getType(),
                    unqualifiedAttribute)).getQualifiedValue());
    }

    AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
        return convertFromString(fieldMapping.getTarget().getType(),
            fieldPrefixFunction.reverse(qualifiedAttribute.getS()).getUnqualifiedValue());
    }

    private String convertToStringNotNull(ScalarAttributeType type, AttributeValue attributeValue) {
        String convertedString = convertToString(type, attributeValue);
        checkNotNull(convertedString, "attributeValue=" + attributeValue
                     + " of type=" + type.name() + " could not be converted");
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
                String decodedString = UTF_8.decode(attributeValue.getB()).toString();
                attributeValue.getB().rewind(); // rewind so future readers don't get an empty buffer
                return decodedString;
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