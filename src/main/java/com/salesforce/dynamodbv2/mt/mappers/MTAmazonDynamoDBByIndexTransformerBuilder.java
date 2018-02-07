/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDBAdminUtils;
import com.salesforce.dynamodbv2.mt.cache.MTCache;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndexTransformer.LocalTableDescription;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Builds MTAmazonDynamoDBByIndexTransformer's for a given Dynamo virtual table definition.  The MTAmazonDynamoDBByIndexTransformer
 * contains the state necessary to transform query, scan, get|put|update|deleteItem requests and results to/from their
 * underlying physical table.
 *
 * @author msgroi
 */
class MTAmazonDynamoDBByIndexTransformerBuilder {

    private static final String DATA_HK_S_TABLENAME = "_DATA_HK_S";
    private static final String DATA_HK_S_RK_S_TABLENAME = "_DATA_HK_S_RK_S";
    private static final String DATA_TABLENAME_HKFIELD = "hk";
    private static final String DATA_TABLENAME_GSI_S_FIELD = "gsi_s";
    private static final String DATA_TABLENAME_GSI_INDEX_NAME = "gsi";
    private static final String DATA_TABLENAME_RKFIELD = "rk";
    private static final String DATA_TABLENAME_LSI_S_FIELD = "lsi_s";
    private static final String DATA_TABLENAME_LSI_INDEX_NAME = "lsi";
    private final AmazonDynamoDB amazonDynamoDB;
    private final AmazonDynamoDBAdminUtils adminUtils;
    private final MTAmazonDynamoDBContextProvider mtContext;
    private final String delimiter;
    private final int pollIntervalSeconds;
    private final Optional<String> tablePrefix;
    private final MTCache<MTAmazonDynamoDBByIndexTransformer> indexTransformerCache;
    private final long tableRCU;
    private final long tableWCU;
    private final long gsiRCU;
    private final long gsiWCU;

    MTAmazonDynamoDBByIndexTransformerBuilder(AmazonDynamoDB amazonDynamoDB,
                                              MTAmazonDynamoDBContextProvider mtContext,
                                              String delimiter,
                                              int pollIntervalSeconds,
                                              Optional<String> tablePrefix,
                                              long tableRCU,
                                              long tableWCU,
                                              long gsiRCU,
                                              long gsiWCU) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.mtContext = mtContext;
        this.delimiter = delimiter;
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.tablePrefix = tablePrefix;
        this.adminUtils = new AmazonDynamoDBAdminUtils(amazonDynamoDB);
        this.indexTransformerCache = new MTCache<>(mtContext);
        this.tableRCU = tableRCU;
        this.tableWCU = tableWCU;
        this.gsiRCU = gsiRCU;
        this.gsiWCU = gsiWCU;
    }

    MTAmazonDynamoDBByIndexTransformer getIndexTransformer(TableDescription dynamoVirtualTableDescription) {
        try {
            return indexTransformerCache.get(dynamoVirtualTableDescription.getTableName(),
                    () -> buildIndexTransformer(dynamoVirtualTableDescription));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    void invalidateTransformerCache(String tableName) {
        this.indexTransformerCache.invalidate(tableName);
    }

    private MTAmazonDynamoDBByIndexTransformer buildIndexTransformer(TableDescription dynamoVirtualTableDescription) {
        LocalTableDescription virtualTableDescription = new LocalTableDescription(dynamoVirtualTableDescription);
        LocalTableDescription physicalTableDescription = getPhysicalTableDescription(virtualTableDescription);
        return new MTAmazonDynamoDBByIndexTransformer(virtualTableDescription,
                physicalTableDescription,
                mtContext,
                delimiter);
    }

    private LocalTableDescription getPhysicalTableDescription(LocalTableDescription virtualTableDescription) {
        String physicalTableName = virtualTableDescription.getRangeKey().isPresent() ? DATA_HK_S_RK_S_TABLENAME : DATA_HK_S_TABLENAME;
        String prefixPhysicalTableName = prefix(physicalTableName);
        try {
            return new LocalTableDescription(amazonDynamoDB.describeTable(prefixPhysicalTableName).getTable());
        } catch (ResourceNotFoundException e) {
            adminUtils.createTableIfNotExists(buildCreateTableRequest(physicalTableName, prefixPhysicalTableName), pollIntervalSeconds);
            return getPhysicalTableDescription(virtualTableDescription);
        }
    }

    private CreateTableRequest buildCreateTableRequest(String physicalTableName, String prefixedPhysicalTableName) {
        if (physicalTableName.equals(DATA_HK_S_TABLENAME)) {
            return new CreateTableRequest()
                            .withTableName(prefixedPhysicalTableName)
                            .withKeySchema(new KeySchemaElement().withAttributeName(DATA_TABLENAME_HKFIELD).withKeyType(KeyType.HASH))
                            .withAttributeDefinitions(new AttributeDefinition().withAttributeName(DATA_TABLENAME_HKFIELD).withAttributeType(ScalarAttributeType.S),
                                                      new AttributeDefinition().withAttributeName(DATA_TABLENAME_GSI_S_FIELD).withAttributeType(ScalarAttributeType.S))
                            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName(DATA_TABLENAME_GSI_INDEX_NAME)
                                                                                  .withKeySchema(new KeySchemaElement(DATA_TABLENAME_GSI_S_FIELD, KeyType.HASH))
                                                                                  .withProvisionedThroughput(new ProvisionedThroughput(gsiRCU, gsiWCU))
                                                                                  .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                            .withProvisionedThroughput(new ProvisionedThroughput(tableRCU, tableWCU));
        }
        if (physicalTableName.equals(DATA_HK_S_RK_S_TABLENAME)) {
            return new CreateTableRequest()
                            .withTableName(prefixedPhysicalTableName)
                            .withKeySchema(new KeySchemaElement().withAttributeName(DATA_TABLENAME_HKFIELD).withKeyType(KeyType.HASH),
                                    new KeySchemaElement().withAttributeName(DATA_TABLENAME_RKFIELD).withKeyType(KeyType.RANGE))
                            .withAttributeDefinitions(new AttributeDefinition().withAttributeName(DATA_TABLENAME_HKFIELD).withAttributeType(ScalarAttributeType.S),
                                    new AttributeDefinition().withAttributeName(DATA_TABLENAME_RKFIELD).withAttributeType(ScalarAttributeType.S),
                                    new AttributeDefinition().withAttributeName(DATA_TABLENAME_GSI_S_FIELD).withAttributeType(ScalarAttributeType.S),
                                    new AttributeDefinition().withAttributeName(DATA_TABLENAME_LSI_S_FIELD).withAttributeType(ScalarAttributeType.S))
                            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName(DATA_TABLENAME_GSI_INDEX_NAME)
                                                                                  .withKeySchema(new KeySchemaElement(DATA_TABLENAME_GSI_S_FIELD, KeyType.HASH))
                                                                                  .withProvisionedThroughput(new ProvisionedThroughput(gsiRCU, gsiWCU))
                                                                                  .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                            .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName(DATA_TABLENAME_LSI_INDEX_NAME)
                                                                                .withKeySchema(new KeySchemaElement(DATA_TABLENAME_HKFIELD, KeyType.HASH),
                                                                                               new KeySchemaElement(DATA_TABLENAME_LSI_S_FIELD, KeyType.RANGE))
                                                                                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                            .withProvisionedThroughput(new ProvisionedThroughput(tableRCU, tableWCU));
        }
        throw new RuntimeException("unsupported table=" + physicalTableName + " encountered");
    }

    private String prefix(String tableName) {
        return tablePrefix.map(tablePrefix -> tablePrefix + tableName).orElse(tableName);
    }

}