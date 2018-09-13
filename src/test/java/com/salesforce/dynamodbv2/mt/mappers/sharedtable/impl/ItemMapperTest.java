/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class ItemMapperTest {

    private static final String PREFIX = "PREFIX-";
    private static final ItemMapper SUT = new ItemMapper(new TableMapping(new DynamoTableDescriptionImpl(
        CreateTableRequestBuilder.builder().withTableKeySchema("virtualhk", S).build()),
            new SingletonCreateTableRequestFactory(new DynamoTableDescriptionImpl(CreateTableRequestBuilder.builder()
            .withTableKeySchema("physicalhk", S).build())
            .getCreateTableRequest()),
        new DynamoSecondaryIndexMapperByTypeImpl(),
        null,
        null
    ), new MockFieldMapper());

    @Test
    void applyAndReverse() {
        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualhk", new AttributeValue().withS("hkvalue"),
            "somefield", new AttributeValue().withS("somevalue"));

        Map<String, AttributeValue> mappedItem = SUT.apply(item);

        assertEquals(ImmutableMap.of(
            "physicalhk", new AttributeValue().withS(PREFIX + "hkvalue"),
            "somefield", new AttributeValue().withS("somevalue")), mappedItem);

        Map<String, AttributeValue> reversedItem = SUT.reverse(mappedItem);

        assertEquals(item, reversedItem);
    }

    @Test
    void reverseNull() {
        assertNull(SUT.reverse(null));
    }

    private static class MockFieldMapper extends FieldMapper {

        MockFieldMapper() {
            super(null, null, null);
        }

        @Override
        AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
            if (unqualifiedAttribute.getS() != null) {
                return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getS());
            }
            if (unqualifiedAttribute.getN() != null) {
                return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getN());
            }
            return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getB());
        }

        @Override
        AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
            return new AttributeValue().withS(qualifiedAttribute.getS().substring(7));
        }
    }

}