/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class DynamoSecondaryIndexTest {

    @Test
    void hk() {
        DynamoSecondaryIndex sut = new DynamoSecondaryIndex(
            ImmutableList.of(new AttributeDefinition().withAttributeName("hk").withAttributeType(S)),
            "index1",
            ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH)),
            GSI);
        PrimaryKey primaryKey = sut.getPrimaryKey();
        assertEquals("hk", primaryKey.getHashKey());
        assertEquals(S, primaryKey.getHashKeyType());
        assertEquals(empty(), primaryKey.getRangeKey());
        assertEquals(empty(), primaryKey.getRangeKeyType());
        assertEquals("index1", sut.getIndexName());
        assertEquals(GSI, sut.getType());
    }

    @Test
    void hashKeyAndRangeKey() {
        DynamoSecondaryIndex sut = new DynamoSecondaryIndex(
            ImmutableList.of(new AttributeDefinition().withAttributeName("hk").withAttributeType(S),
                new AttributeDefinition().withAttributeName("rk").withAttributeType(N)),
            "index2",
            ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH),
                new KeySchemaElement().withAttributeName("rk").withKeyType(RANGE)),
            LSI);
        PrimaryKey primaryKey = sut.getPrimaryKey();
        assertEquals("hk", primaryKey.getHashKey());
        assertEquals(S, primaryKey.getHashKeyType());
        assertEquals("rk", primaryKey.getRangeKey().get());
        assertEquals(N, primaryKey.getRangeKeyType().get());
        assertEquals("index2", sut.getIndexName());
        assertEquals(LSI, sut.getType());
        assertNotNull(sut.toString());
    }

    @Test
    void missingAttributeDefinition() {
        try {
            new DynamoSecondaryIndex(
                ImmutableList.of(new AttributeDefinition().withAttributeName("foo").withAttributeType(S)),
                "index3",
                ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH)),
                GSI);
        } catch (IllegalArgumentException ignore) {
            // expected
        }
    }

}