/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.metadata;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class DynamoTableDescriptionImpl implements DynamoTableDescription {

    private final String tableName;
    private final Set<AttributeDefinition> attributeDefinitions;
    private final PrimaryKey primaryKey;
    private final Map<String, DynamoSecondaryIndex> gsiMap;
    private final Map<String, DynamoSecondaryIndex> lsiMap;
    private final StreamSpecification streamSpecification;
    private final String lastStreamArn;

    private final CreateTableRequest createTableRequest;

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequest the {@code CreateTableRequest} being wrapped
     */
    public DynamoTableDescriptionImpl(CreateTableRequest createTableRequest) {
        this.createTableRequest = createTableRequest;
        tableName = createTableRequest.getTableName();
        List<AttributeDefinition> attributeDefinitionsList = createTableRequest.getAttributeDefinitions();
        this.attributeDefinitions = new HashSet<>(attributeDefinitionsList);
        primaryKey = getPrimaryKey(createTableRequest.getKeySchema());
        gsiMap = createTableRequest.getGlobalSecondaryIndexes() == null ? new HashMap<>() :
            createTableRequest.getGlobalSecondaryIndexes().stream().map(gsi ->
                new DynamoSecondaryIndex(attributeDefinitionsList,
                    gsi.getIndexName(), gsi.getKeySchema(), GSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        lsiMap = createTableRequest.getLocalSecondaryIndexes() == null ? new HashMap<>() :
            createTableRequest.getLocalSecondaryIndexes().stream().map(lsi ->
                new DynamoSecondaryIndex(attributeDefinitionsList,
                    lsi.getIndexName(), lsi.getKeySchema(), LSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        streamSpecification = createTableRequest.getStreamSpecification();
        lastStreamArn = null;
    }

    /**
     * TODO: write Javadoc.
     *
     * @param tableDescription the {@code TableDescription} being wrapped
     */
    public DynamoTableDescriptionImpl(TableDescription tableDescription) {
        this.createTableRequest = null;
        tableName = tableDescription.getTableName();
        List<AttributeDefinition> attributeDefinitionsList = tableDescription.getAttributeDefinitions();
        this.attributeDefinitions = new HashSet<>(attributeDefinitionsList);
        primaryKey = getPrimaryKey(tableDescription.getKeySchema());
        gsiMap = tableDescription.getGlobalSecondaryIndexes() == null ? new HashMap<>() :
            tableDescription.getGlobalSecondaryIndexes().stream().map(gsi ->
                new DynamoSecondaryIndex(attributeDefinitionsList, gsi.getIndexName(), gsi.getKeySchema(), GSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        lsiMap = tableDescription.getLocalSecondaryIndexes() == null ? new HashMap<>() :
            tableDescription.getLocalSecondaryIndexes().stream().map(lsi ->
                new DynamoSecondaryIndex(attributeDefinitionsList, lsi.getIndexName(), lsi.getKeySchema(), LSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        lastStreamArn = tableDescription.getLatestStreamArn();
        streamSpecification = tableDescription.getStreamSpecification();
    }

    private PrimaryKey getPrimaryKey(List<KeySchemaElement> keySchema) {
        KeySchemaElement hashKeySchema = getKeySchemaElement(keySchema, HASH)
            .orElseThrow(() -> new IllegalArgumentException("no HASH found in " + keySchema));
        Optional<KeySchemaElement> rangeKeySchema = getKeySchemaElement(keySchema, RANGE);
        return new PrimaryKey(hashKeySchema.getAttributeName(),
            getAttributeType(hashKeySchema.getAttributeName()),
            rangeKeySchema.map(KeySchemaElement::getAttributeName),
            rangeKeySchema.map(keySchemaElement -> getAttributeType(keySchemaElement.getAttributeName())));
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    private Optional<KeySchemaElement> getKeySchemaElement(List<KeySchemaElement> keySchema, KeyType keyType) {
        checkNotNull(keySchema, "keySchema is required");
        return keySchema.stream()
            .filter(keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == keyType)
            .findFirst();
    }

    private ScalarAttributeType getAttributeType(String attributeName) {
        return ScalarAttributeType.valueOf(attributeDefinitions.stream()
            .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(attributeName))
            .findFirst().orElseThrow(() -> new IllegalArgumentException("attribute with name '" + attributeName
                + "' not found in " + attributeDefinitions)).getAttributeType());
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<DynamoSecondaryIndex> getSis() {
        List<DynamoSecondaryIndex> sis = new ArrayList<>();
        sis.addAll(gsiMap.values());
        sis.addAll(lsiMap.values());
        return sis;
    }

    @Override
    public List<DynamoSecondaryIndex> getGsis() {
        return new ArrayList<>(gsiMap.values());
    }

    @Override
    public Optional<DynamoSecondaryIndex> getGsi(String indexName) {
        return Optional.ofNullable(gsiMap.get(indexName));
    }

    @Override
    public List<DynamoSecondaryIndex> getLsis() {
        return new ArrayList<>(lsiMap.values());
    }

    @Override
    public DynamoSecondaryIndex findSi(String indexName) {
        Optional<DynamoSecondaryIndex> si = getGsi(indexName);
        if (si.isEmpty()) {
            si = getLsi(indexName);
        }
        if (si.isEmpty()) {
            throw new IllegalArgumentException("secondary index '" + indexName
                + "' not found on table " + this.getTableName());
        }
        return si.get();
    }

    @Override
    public StreamSpecification getStreamSpecification() {
        return streamSpecification;
    }

    @Override
    public String getLastStreamArn() {
        return lastStreamArn;
    }

    @Override
    public CreateTableRequest getCreateTableRequest() {
        return createTableRequest;
    }

    @Override
    public String toString() {
        return "{"
            + "tableName='" + tableName + '\''
            + ", attributeDefinitions=" + attributeDefinitions
            + ", primaryKey=" + primaryKey
            + ", gsiMap=" + gsiMap.values()
            + ", lsiMap=" + lsiMap.values()
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

        final DynamoTableDescriptionImpl that = (DynamoTableDescriptionImpl) o;

        return Objects.equals(streamSpecification, that.streamSpecification) && tableName.equals(that.tableName)
            && attributeDefinitions.equals(that.attributeDefinitions)
            && primaryKey.equals(that.primaryKey)
            && gsiMap.equals(that.gsiMap)
            && lsiMap.equals(that.lsiMap);

    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (attributeDefinitions != null ? attributeDefinitions.hashCode() : 0);
        result = 31 * result + (primaryKey != null ? primaryKey.hashCode() : 0);
        result = 31 * result + (gsiMap != null ? gsiMap.hashCode() : 0);
        result = 31 * result + (lsiMap != null ? lsiMap.hashCode() : 0);
        result = 31 * result + (streamSpecification != null ? streamSpecification.hashCode() : 0);
        return result;
    }

    private Optional<DynamoSecondaryIndex> getLsi(String indexName) {
        return Optional.ofNullable(lsiMap.get(indexName));
    }

}