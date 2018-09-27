/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.metadata;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import com.google.common.base.Preconditions;
import java.util.Optional;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class PrimaryKey {

    private final String hashKey;
    private final ScalarAttributeType hashKeyType;
    private final Optional<String> rangeKey;
    private final Optional<ScalarAttributeType> rangeKeyType;

    public PrimaryKey(String hashKey,
                      ScalarAttributeType hashKeyType) {
        this(hashKey, hashKeyType, Optional.empty(), Optional.empty());
    }

    public PrimaryKey(String hashKey,
                      ScalarAttributeType hashKeyType,
                      String rangeKey,
                      ScalarAttributeType rangeKeyType) {
        this(hashKey, hashKeyType, Optional.ofNullable(rangeKey), Optional.ofNullable(rangeKeyType));
    }

    /**
     * TODO: write Javadoc.
     *
     * @param hashKey the hash-key value
     * @param hashKeyType the type of the hash-key field
     * @param rangeKey the range-key value
     * @param rangeKeyType the type of the range-key field
     */
    public PrimaryKey(String hashKey,
                      ScalarAttributeType hashKeyType,
                      Optional<String> rangeKey,
                      Optional<ScalarAttributeType> rangeKeyType) {
        Preconditions.checkState(rangeKey.isPresent() == rangeKeyType.isPresent(),
            "rangeKey and rangeKeyType must both be present or both be absent");
        this.hashKey = hashKey;
        this.hashKeyType = hashKeyType;
        this.rangeKey = rangeKey;
        this.rangeKeyType = rangeKeyType;
    }

    public String getHashKey() {
        return hashKey;
    }

    public ScalarAttributeType getHashKeyType() {
        return hashKeyType;
    }

    public Optional<String> getRangeKey() {
        return rangeKey;
    }

    public Optional<ScalarAttributeType> getRangeKeyType() {
        return rangeKeyType;
    }

    @Override
    public String toString() {
        return "{"
            + "hashKey='" + hashKey + '\''
            + ", hashKeyType=" + hashKeyType
            + (rangeKey.map(s -> ", rangeKey=" + s + ", rangeKeyType=" + rangeKeyType.get()).orElse(""))
            + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimaryKey that = (PrimaryKey) o;

        return hashKey.equals(that.hashKey)
                && hashKeyType == that.hashKeyType
                && rangeKey.equals(that.rangeKey)
                && rangeKeyType.equals(that.rangeKeyType);
    }

    @Override
    public int hashCode() {
        int result = hashKey.hashCode();
        result = 31 * result + hashKeyType.hashCode();
        result = 31 * result + rangeKey.hashCode();
        result = 31 * result + rangeKeyType.hashCode();
        return result;
    }
}