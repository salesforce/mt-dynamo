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
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class RandomPartitioningItemMapperTest {

    private static final String PREFIX = "PREFIX-";

    private static final RandomPartitioningTableMapping tableMapping =  new RandomPartitioningTableMapping(
        new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableKeySchema("virtualHk", S, "virtualRk", S).build()),
        new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableKeySchema("physicalHk", S, "physicalRk", S).build()),
        index -> null,
        null
    );
    private static final RandomPartitioningItemMapper SUT = new RandomPartitioningItemMapper(
        new MockFieldMapper(),
        tableMapping.getTablePrimaryKeyFieldMappings(),
        Collections.emptyMap(),
        tableMapping.getAllMappingsPerField());

    @Test
    void applyAndReverse() {
        Map<String, AttributeValue> item = ImmutableMap.of(
            "virtualHk", new AttributeValue().withS("hkValue"),
            "virtualRk", new AttributeValue().withS("rkValue"),
            "someField", new AttributeValue().withS("someValue"));

        Map<String, AttributeValue> mappedItem = SUT.applyForWrite(item);

        assertEquals(ImmutableMap.of(
            "physicalHk", new AttributeValue().withS(PREFIX + "hkValue"),
            "physicalRk", new AttributeValue().withS("rkValue"),
            "someField", new AttributeValue().withS("someValue")), mappedItem);

        Map<String, AttributeValue> reversedItem = SUT.reverse(mappedItem);

        assertEquals(item, reversedItem);
    }

    @Test
    void reverseNull() {
        assertNull(SUT.reverse(null));
    }

    private static class MockFieldMapper implements FieldMapper {

        MockFieldMapper() {
            super();
        }

        @Override
        public AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
            if (unqualifiedAttribute.getS() != null) {
                return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getS());
            }
            if (unqualifiedAttribute.getN() != null) {
                return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getN());
            }
            return new AttributeValue().withS(PREFIX + unqualifiedAttribute.getB());
        }

        @Override
        public AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
            return new AttributeValue().withS(qualifiedAttribute.getS().substring(7));
        }

        @Override
        public Predicate<AttributeValue> createFilter() {
            return v -> true;
        }
    }

}