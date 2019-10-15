/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * Represents the primary or secondary index being used in a request.
 */
class RequestIndex {

    private final PrimaryKey virtualPk;
    private final PrimaryKey physicalPk;
    private final Optional<DynamoSecondaryIndex> virtualSecondaryIndex;
    private final Optional<DynamoSecondaryIndex> physicalSecondaryIndex;

    private RequestIndex(PrimaryKey virtualPk, PrimaryKey physicalPk,
                         Optional<DynamoSecondaryIndex> virtualSecondaryIndex,
                         Optional<DynamoSecondaryIndex> physicalSecondaryIndex) {
        this.virtualPk = virtualPk;
        this.physicalPk = physicalPk;
        this.virtualSecondaryIndex = virtualSecondaryIndex;
        this.physicalSecondaryIndex = physicalSecondaryIndex;
    }

    static RequestIndex fromVirtualSecondaryIndexName(DynamoTableDescription virtualTable,
                                                      DynamoTableDescription physicalTable,
                                                      UnaryOperator<DynamoSecondaryIndex> secondaryIndexMapper,
                                                      @Nullable String indexName) {
        if (indexName == null) {
            return new RequestIndex(virtualTable.getPrimaryKey(), physicalTable.getPrimaryKey(), Optional.empty(),
                Optional.empty());
        } else {
            DynamoSecondaryIndex virtualSecondaryIndex = virtualTable.findSi(indexName);
            checkArgument(virtualSecondaryIndex != null, "No such virtual secondary index exists: %s", indexName);
            DynamoSecondaryIndex physicalSecondaryIndex = secondaryIndexMapper.apply(virtualSecondaryIndex);
            checkArgument(physicalSecondaryIndex != null, "No corresponding physical secondary index: %s",
                physicalSecondaryIndex);
            return new RequestIndex(virtualSecondaryIndex.getPrimaryKey(), physicalSecondaryIndex.getPrimaryKey(),
                Optional.of(virtualSecondaryIndex), Optional.of(physicalSecondaryIndex));
        }
    }

    PrimaryKey getVirtualPk() {
        return virtualPk;
    }

    PrimaryKey getPhysicalPk() {
        return physicalPk;
    }

    Optional<DynamoSecondaryIndex> getVirtualSecondaryIndex() {
        return virtualSecondaryIndex;
    }

    Optional<DynamoSecondaryIndex> getPhysicalSecondaryIndex() {
        return physicalSecondaryIndex;
    }
}
