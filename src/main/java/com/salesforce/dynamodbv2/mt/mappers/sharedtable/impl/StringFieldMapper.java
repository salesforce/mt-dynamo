/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Adds and removes prefixes to fields based on the tenant context.
 *
 * @author msgroi
 */
class StringFieldMapper implements FieldMapper {

    private static final FieldPrefixFunction<String> fieldPrefixFunction = new StringFieldPrefixFunction();

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final String virtualTableName;

    StringFieldMapper(MtAmazonDynamoDbContextProvider mtContext,
                      String virtualTableName) {
        this.mtContext = mtContext;
        this.virtualTableName = virtualTableName;
    }

    @Override
    public AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
        checkArgument(fieldMapping.getTarget().getType() == S);
        String stringValue = convertToStringNotNull(fieldMapping.getSource().getType(), unqualifiedAttribute);
        FieldValue<String> fieldValue = new FieldValue<>(mtContext.getContext(), virtualTableName, stringValue);
        return new AttributeValue(fieldPrefixFunction.apply(fieldValue));
    }

    @Override
    public AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
        checkArgument(fieldMapping.getSource().getType() == S);
        FieldValue<String> fieldValue = fieldPrefixFunction.reverse(qualifiedAttribute.getS());
        return convertFromString(fieldMapping.getTarget().getType(), fieldValue.getValue());
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
                return Base64.getEncoder().encodeToString(attributeValue.getB().array());
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
                return unqualifiedAttribute.withB(ByteBuffer.wrap(Base64.getDecoder().decode(value)));
            default:
                throw new IllegalArgumentException("unexpected type " + type + " encountered");
        }
    }

}