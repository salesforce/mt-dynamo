/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;

import java.util.List;
import java.util.Optional;

/**
 * Model class representing secondary index.
 *
 * @author msgroi
 */
public class DynamoSecondaryIndex implements HasPrimaryKey {

    private final String indexName;
    private final PrimaryKey primaryKey;
    private final DynamoSecondaryIndexType type;

    /**
     * TODO: write Javadoc.
     *
     * @param attributeDefinitions the index's attribute definitions
     * @param indexName the name of the index
     * @param primaryKey the primary key of the index
     * @param type the type of index, GSI vs. LSI
     */
    public DynamoSecondaryIndex(List<AttributeDefinition> attributeDefinitions,
                                String indexName,
                                List<KeySchemaElement> primaryKey,
                                DynamoSecondaryIndexType type) {
        this.indexName = indexName;
        this.type = type;
        this.primaryKey = getPrimaryKey(primaryKey, attributeDefinitions);
    }

    public String getIndexName() {
        return indexName;
    }

    public DynamoSecondaryIndexType getType() {
        return type;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * TODO: write Javadoc.
     */
    private PrimaryKey getPrimaryKey(List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions) {
        KeySchemaElement hashKeySchema = getKeySchemaElement(keySchema, HASH)
            .orElseThrow(() -> new IllegalArgumentException("no HASH found in " + keySchema));
        Optional<KeySchemaElement> rangeKeySchema = getKeySchemaElement(keySchema, RANGE);
        return new PrimaryKey(hashKeySchema.getAttributeName(),
            getAttributeType(hashKeySchema.getAttributeName(), attributeDefinitions),
            rangeKeySchema.map(KeySchemaElement::getAttributeName),
            rangeKeySchema.map(keySchemaElement -> getAttributeType(keySchemaElement.getAttributeName(),
                attributeDefinitions)));
    }

    private Optional<KeySchemaElement> getKeySchemaElement(List<KeySchemaElement> keySchema, KeyType keyType) {
        return keySchema.stream()
            .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == keyType)
            .findFirst();
    }

    private ScalarAttributeType getAttributeType(String attributeName, List<AttributeDefinition> attributeDefinitions) {
        return ScalarAttributeType.valueOf(attributeDefinitions.stream()
            .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(attributeName))
            .findFirst().orElseThrow(() ->
                new IllegalArgumentException("attribute with name '" + attributeName + "' not found in "
                    + attributeDefinitions)).getAttributeType());
    }

    public enum DynamoSecondaryIndexType {
        GSI,
        LSI
    }

    @Override
    public String toString() {
        return "{"
            + "indexName='" + indexName + '\''
            + ", keySchema='" + getPrimaryKey()
            + ", type=" + type.name()
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DynamoSecondaryIndex that = (DynamoSecondaryIndex) o;

        return indexName.equals(that.indexName)
                && primaryKey.equals(that.primaryKey)
                && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = indexName.hashCode();
        result = 31 * result + primaryKey.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

}