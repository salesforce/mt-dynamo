/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARYINDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
// TODO msgroi add coverage
class ItemMapperTest {

    private static final String PREFIX = "PREFIX-";
    private static final char DELIMITER = '.';
    private static final ItemMapper SUT = new ItemMapper(
            new MockFieldMapper(),
            new TableMapping(new DynamoTableDescriptionImpl(
                    CreateTableRequestBuilder.builder().withTableKeySchema("virtualhk", S).build()),
                    new SingletonCreateTableRequestFactory(new DynamoTableDescriptionImpl(
                            CreateTableRequestBuilder.builder()
                                    .withTableKeySchema("physicalhk", S).build())
                            .getCreateTableRequest()),
                    new DynamoSecondaryIndexMapperByTypeImpl(),
                    null,
                    DELIMITER
            ).getAllVirtualToPhysicalFieldMappings());

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
    @Disabled
    // TODO msgroi test
    void getAllPhysicalToVirtualFieldMappings() {
        assertEquals(ImmutableMap.of(
                "physicalhk", ImmutableList.of(new FieldMapping(
                        new FieldMapping.Field("physicalhk", S),
                        new Field("virtualhk", N),
                        "virtualTableName",
                        "physicalTableName",
                        TABLE,
                        true)),
                "physicalrk", ImmutableList.of(new FieldMapping(
                        new Field("physicalrk", N),
                        new Field("virtualrk", N),
                        "virtualTableName",
                        "physicalTableName",
                        TABLE,
                        false)),
                "physicalgsihk", ImmutableList.of(new FieldMapping(new Field("physicalgsihk", S),
                        new Field("virtualgsihk", S),
                        "virtualgsi",
                        "physicalgsi",
                        SECONDARYINDEX,
                        true)),
                "physicalgsirk", ImmutableList.of(new FieldMapping(new Field("physicalgsirk", N),
                        new Field("virtualgsirk", N),
                        "virtualgsi",
                        "physicalgsi",
                        SECONDARYINDEX,
                        false))),
                //SUT.getAllPhysicalToVirtualFieldMappings()
                null
        );
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