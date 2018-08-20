/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class DynamoSecondaryIndexMapperByTypeImplTest {

    @Test
    void testMatch() throws MappingException {
        DynamoSecondaryIndex vsi = new DynamoSecondaryIndex(
            ImmutableList.of(new AttributeDefinition().withAttributeName("hk").withAttributeType(S)),
            "index1",
            ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH)),
            GSI);
        DynamoTableDescription physicalTable = mock(DynamoTableDescription.class);
        when(physicalTable.getSis()).thenReturn(ImmutableList.of(vsi));
        new DynamoSecondaryIndexMapperByTypeImpl().lookupPhysicalSecondaryIndex(vsi, physicalTable);
    }


    @Test
    void testNoMatch() {
        DynamoSecondaryIndex vsi = new DynamoSecondaryIndex(
            ImmutableList.of(new AttributeDefinition().withAttributeName("hk").withAttributeType(S)),
            "index1",
            ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH)),
            GSI);
        DynamoTableDescription physicalTable = mock(DynamoTableDescription.class);
        when(physicalTable.getSis()).thenReturn(ImmutableList.of(new DynamoSecondaryIndex(
            ImmutableList.of(new AttributeDefinition().withAttributeName("hk").withAttributeType(N)),
            "index1",
            ImmutableList.of(new KeySchemaElement().withAttributeName("hk").withKeyType(HASH)),
            GSI)));
        try {
            new DynamoSecondaryIndexMapperByTypeImpl().lookupPhysicalSecondaryIndex(vsi, physicalTable);
        } catch (MappingException ignore) {
            // expected
        }
    }

}