/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DynamoTableDescriptionImplTest {

    @ParameterizedTest(name = "{index}")
    @MethodSource("testEqualsData")
    void testEquals(DynamoTableDescriptionImpl tableDescription1,
                    DynamoTableDescriptionImpl tableDescription2,
                    boolean expected) {
        assertEquals(expected, tableDescription1.equals(tableDescription2));
    }

    private static Stream<Arguments> testEqualsData() {
        CreateTableRequest createTableRequestWithReversedAttributeDefinitions = buildCreateTableRequest();
        List<AttributeDefinition> reversedAttributeDefinitions =
            new ArrayList<>(createTableRequestWithReversedAttributeDefinitions.getAttributeDefinitions());
        Collections.reverse(reversedAttributeDefinitions);
        createTableRequestWithReversedAttributeDefinitions.withAttributeDefinitions(reversedAttributeDefinitions);
        DynamoTableDescriptionImpl dynamoTableDescription = new DynamoTableDescriptionImpl(buildCreateTableRequest());
        return Stream.of(
            Arguments.of(dynamoTableDescription, new DynamoTableDescriptionImpl(buildCreateTableRequest()), true),
            Arguments.of(dynamoTableDescription,
                new DynamoTableDescriptionImpl(buildCreateTableRequest().withTableName("anothertable")), false),
            Arguments.of(dynamoTableDescription,
                new DynamoTableDescriptionImpl(createTableRequestWithReversedAttributeDefinitions), true),
            Arguments.of(dynamoTableDescription,
                new DynamoTableDescriptionImpl(buildCreateTableRequest()
                    .withAttributeDefinitions(
                        new AttributeDefinition("anotherhk", ScalarAttributeType.S))), false),
            Arguments.of(dynamoTableDescription, new DynamoTableDescriptionImpl(
                buildCreateTableRequest()
                    .withAttributeDefinitions(
                        ImmutableList.of(new AttributeDefinition("anotherhk", ScalarAttributeType.S),
                            new AttributeDefinition("gsihk", ScalarAttributeType.S),
                            new AttributeDefinition("lsihk", ScalarAttributeType.S)))
                    .withKeySchema(ImmutableList.of(
                        new KeySchemaElement().withAttributeName("anotherhk").withKeyType(KeyType.HASH)))), false),
            Arguments.of(dynamoTableDescription, new DynamoTableDescriptionImpl(
                buildCreateTableRequest().withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                    .withIndexName("anothergsi")
                    .withKeySchema(new KeySchemaElement()
                        .withAttributeName("gsihk").withKeyType(KeyType.HASH)))), false),
            Arguments.of(dynamoTableDescription, new DynamoTableDescriptionImpl(
                buildCreateTableRequest().withLocalSecondaryIndexes(new LocalSecondaryIndex()
                    .withIndexName("anotherlsi")
                    .withKeySchema(new KeySchemaElement()
                        .withAttributeName("lsihk").withKeyType(KeyType.HASH)))), false),
            Arguments.of(dynamoTableDescription, new DynamoTableDescriptionImpl(
                buildCreateTableRequest().withStreamSpecification(
                    new StreamSpecification().withStreamEnabled(true))), false)
        );
    }

    private static CreateTableRequest buildCreateTableRequest() {
        return new CreateTableRequest()
            .withTableName("table")
            .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S),
                new AttributeDefinition("gsihk", ScalarAttributeType.S),
                new AttributeDefinition("lsihk", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement().withAttributeName("hk").withKeyType(KeyType.HASH))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                .withIndexName("gsi")
                .withKeySchema(new KeySchemaElement().withAttributeName("gsihk").withKeyType(KeyType.HASH)))
            .withLocalSecondaryIndexes(new LocalSecondaryIndex()
                .withIndexName("lsi")
                .withKeySchema(new KeySchemaElement().withAttributeName("lsihk").withKeyType(KeyType.HASH)))
            .withStreamSpecification(new StreamSpecification().withStreamEnabled(false));
    }

}
