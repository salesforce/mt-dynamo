/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;

import com.google.common.base.Joiner;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Looks for the first candidate HasPrimaryKey where the hash key type and the range key type are both binary.
 */
public class PrimaryKeyMapperToBinary implements PrimaryKeyMapper {

    @Override
    public HasPrimaryKey mapPrimaryKey(PrimaryKey primaryKeyToFind,
                                       List<HasPrimaryKey> primaryKeys) throws MappingException {
        for (HasPrimaryKey hasPrimaryKey : primaryKeys) {
            PrimaryKey primaryKey = hasPrimaryKey.getPrimaryKey();
            if (primaryKey.getHashKeyType() == B && primaryKey.getRangeKeyType().equals(Optional.of(B))) {
                return hasPrimaryKey;
            }
        }
        throw new MappingException("no key schema compatible with " + primaryKeyToFind
                + " found in list of available keys ["
                + Joiner.on(", ").join(primaryKeys.stream().map(hasPrimaryKey ->
                hasPrimaryKey.getPrimaryKey().toString()).collect(Collectors.toList())) + "]");
    }
}
