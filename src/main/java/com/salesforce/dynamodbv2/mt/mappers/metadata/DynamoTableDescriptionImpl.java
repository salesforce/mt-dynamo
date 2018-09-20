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
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class DynamoTableDescriptionImpl implements DynamoTableDescription {

    private final String tableName;
    private final List<AttributeDefinition> attributeDefinitions;
    private final PrimaryKey primaryKey;
    private final Map<String, DynamoSecondaryIndex> gsiMap;
    private final Map<String, DynamoSecondaryIndex> lsiMap;
    private final ProvisionedThroughputDescription provisionedThroughput;
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
        attributeDefinitions = createTableRequest.getAttributeDefinitions();
        primaryKey = getPrimaryKey(createTableRequest.getKeySchema());
        gsiMap = createTableRequest.getGlobalSecondaryIndexes() == null ? new HashMap<>() :
            createTableRequest.getGlobalSecondaryIndexes().stream().map(gsi ->
                new DynamoSecondaryIndex(attributeDefinitions, gsi.getIndexName(), gsi.getKeySchema(), GSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        lsiMap = createTableRequest.getLocalSecondaryIndexes() == null ? new HashMap<>() :
            createTableRequest.getLocalSecondaryIndexes().stream().map(lsi ->
                new DynamoSecondaryIndex(attributeDefinitions, lsi.getIndexName(), lsi.getKeySchema(), LSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        provisionedThroughput = fromProvisionedThroughput(createTableRequest.getProvisionedThroughput());
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
        attributeDefinitions = tableDescription.getAttributeDefinitions();
        primaryKey = getPrimaryKey(tableDescription.getKeySchema());
        gsiMap = tableDescription.getGlobalSecondaryIndexes() == null ? new HashMap<>() :
            tableDescription.getGlobalSecondaryIndexes().stream().map(gsi ->
                new DynamoSecondaryIndex(attributeDefinitions, gsi.getIndexName(), gsi.getKeySchema(), GSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        lsiMap = tableDescription.getLocalSecondaryIndexes() == null ? new HashMap<>() :
            tableDescription.getLocalSecondaryIndexes().stream().map(lsi ->
                new DynamoSecondaryIndex(attributeDefinitions, lsi.getIndexName(), lsi.getKeySchema(), LSI))
                .collect(Collectors.toMap(DynamoSecondaryIndex::getIndexName, Function.identity()));
        provisionedThroughput = tableDescription.getProvisionedThroughput();
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
    public Optional<DynamoSecondaryIndex> getLsi(String indexName) {
        return Optional.ofNullable(lsiMap.get(indexName));
    }

    @Override
    public DynamoSecondaryIndex findSi(String indexName) {
        Optional<DynamoSecondaryIndex> si = getGsi(indexName);
        if (!si.isPresent()) {
            si = getLsi(indexName);
        }
        if (!si.isPresent()) {
            throw new IllegalArgumentException("secondary index '" + indexName + "' not found");
        }
        return si.get();
    }

    public StreamSpecification getStreamSpecification() {
        return streamSpecification;
    }

    public String getLastStreamArn() {
        return lastStreamArn;
    }

    @Override
    public CreateTableRequest getCreateTableRequest() {
        return createTableRequest;
    }

    @Override
    public String toString() {
        return "DynamoTableDescriptionImpl{"
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

        DynamoTableDescriptionImpl that = (DynamoTableDescriptionImpl) o;

        if (streamSpecification != null
                ? !streamSpecification.equals(that.streamSpecification)
                : that.streamSpecification != null) {
            return false;
        }

        return tableName.equals(that.tableName)
            && attributeDefinitions.equals(that.attributeDefinitions)
            && primaryKey.equals(that.primaryKey)
            && gsiMap.equals(that.gsiMap)
            && lsiMap.equals(that.lsiMap)
            && provisionedThroughput.getReadCapacityUnits()
                .equals(that.provisionedThroughput.getReadCapacityUnits())
            && provisionedThroughput.getWriteCapacityUnits()
                .equals(that.provisionedThroughput.getWriteCapacityUnits());
    }

    private ProvisionedThroughputDescription fromProvisionedThroughput(ProvisionedThroughput provisionedThroughput) {
        return new ProvisionedThroughputDescription()
            .withReadCapacityUnits(provisionedThroughput.getReadCapacityUnits())
            .withWriteCapacityUnits(provisionedThroughput.getWriteCapacityUnits());
    }

}