/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static java.util.Optional.of;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Searches the list of primary keys returning the one that is compatible with the provided primary key according to its
 * HASH and RANGE key types.
 *
 * <p>- look for a primary key in the list that has only a HASH key of type S and a match RANGE key, if the primary key
 *   being found does not have a RANGE key then look for a primary key that doesn't have a RANGE key
 * - if none found and strictMode is not true, then look for a primary key in the list that has both a HASH and RANGE
 *   key of type S, then RANGE key of type N, then RANGE key of type B
 * - otherwise, throw MappingException
 *
 * <p>If any of the above steps finds more than one primary key, then a MappingException is thrown.
 *
 * @author msgroi
 */
public class PrimaryKeyMapperByTypeImpl implements PrimaryKeyMapper {

    private final boolean strictMode;

    public PrimaryKeyMapperByTypeImpl(boolean strictMode) {
        this.strictMode = strictMode;
    }

    @Override
    public HasPrimaryKey mapPrimaryKey(PrimaryKey primaryKeyToFind,
                                       List<HasPrimaryKey> primaryKeys) throws MappingException {
        List<Optional<ScalarAttributeType>> rangeKeyTypePrecedence;
        if (primaryKeyToFind.getRangeKeyType().isPresent()) {
            rangeKeyTypePrecedence = ImmutableList.of(primaryKeyToFind.getRangeKeyType());
        } else {
            rangeKeyTypePrecedence = new ArrayList<>(ImmutableList.of(Optional.empty()));
            if (!strictMode) {
                rangeKeyTypePrecedence.addAll(ImmutableList.of(of(S), of(N), of(B)));
            }
        }
        for (Optional<ScalarAttributeType> rangeKeyType : rangeKeyTypePrecedence) {
            Optional<HasPrimaryKey> primaryKeysFound = mapPrimaryKeyExactMatch(
                new PrimaryKey(primaryKeyToFind.getHashKey(),
                    S,
                    rangeKeyType.map((Function<ScalarAttributeType, String>) Enum::name),
                    rangeKeyType),
                primaryKeys);
            if (primaryKeysFound.isPresent()) {
                return primaryKeysFound.get();
            }
        }
        throw new MappingException("no key schema compatible with " + primaryKeyToFind
            + " found in list of available keys ["
            + Joiner.on(", ").join(primaryKeys.stream().map(hasPrimaryKey ->
            hasPrimaryKey.getPrimaryKey().toString()).collect(Collectors.toList())) + "]");
    }

    private Optional<HasPrimaryKey> mapPrimaryKeyExactMatch(PrimaryKey primaryKeyToFind,
                                                            List<HasPrimaryKey> primaryKeys) throws MappingException {
        List<HasPrimaryKey> primaryKeysFound = primaryKeys.stream().filter(hasPrimaryKey -> {
                // hash key types must match
                PrimaryKey primaryKey = hasPrimaryKey.getPrimaryKey();
                return primaryKey.getHashKeyType().equals(primaryKeyToFind.getHashKeyType())
                    && // either range key doesn't exist on both
                    ((!primaryKeyToFind.getRangeKey().isPresent()
                        && !primaryKey.getRangeKey().isPresent())
                        // or they are present on both and equal
                        || ((primaryKeyToFind.getRangeKeyType().isPresent()
                            && primaryKey.getRangeKeyType().isPresent())
                        && (primaryKeyToFind.getRangeKeyType().get().equals(primaryKey.getRangeKeyType().get()))));
            }
        ).collect(Collectors.toList());
        if (primaryKeysFound.size() > 1) {
            throw new MappingException("when attempting to map from primary key: "
                                       + primaryKeyToFind + ", found multiple compatible primary keys: "
                                       + "[" + Joiner.on(", ").join(primaryKeysFound.stream()
                .map(hasPrimaryKey -> hasPrimaryKey.getPrimaryKey().toString()).collect(Collectors.toList())) + "]");
        }
        return primaryKeysFound.size() == 1 ? of(primaryKeysFound.get(0)) : Optional.empty();
    }

}