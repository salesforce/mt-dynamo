/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.TestAmazonDynamoDBAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomDynamicBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomStaticBuilder;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static java.util.UUID.randomUUID;

/**
 * @author msgroi
 */
class MTAmazonDynamoDBBySharedTableTest {

    private static final boolean randomTableName = false;
    private static final boolean isLocalDynamo = true;
    private static final AmazonDynamoDB rootAmazonDynamoDB = isLocalDynamo
        ? MTAmazonDynamoDBTestRunner.getLocalAmazonDynamoDB()
        : AmazonDynamoDBClientBuilder.standard().build();
    private static final MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();
    private static final AmazonDynamoDB amazonDynamoDB = MTAmazonDynamoDBLogger.builder()
            .withAmazonDynamoDB(rootAmazonDynamoDB)
            .withContext(mtContext)
            .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                    "putItem", "query", "scan", "updateItem")).build();
    private static final boolean randomFieldNames = true;
    private static final String hkTableName = "hkTable";
    private static final String hkRkTableName = "hkRkTable";
    private static final String hashKeyField = random("hashKeyField");
    private static final String rangeKeyField = random("rangeKeyField");
    private static final String indexField = random("indexField");
    private static final String indexRangeField = random("indexRangeField");

    @Test
    void sharedTableCustomDynamic() {
        CreateTableRequestFactory createTableRequestFactory = virtualTableDescription -> {
            String tableName = virtualTableDescription.getTableName();
            if (tableName.endsWith("3")) {
                return new CreateTableRequest()
                        .withTableName(tableName)
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withAttributeDefinitions(new AttributeDefinition(MTAmazonDynamoDBTestRunner.hashKeyField, S),
                                                  new AttributeDefinition(MTAmazonDynamoDBTestRunner.rangeKeyField, S),
                                                  new AttributeDefinition(MTAmazonDynamoDBTestRunner.indexField, S),
                                                  new AttributeDefinition(indexRangeField, S))
                        .withKeySchema(new KeySchemaElement(MTAmazonDynamoDBTestRunner.hashKeyField, KeyType.HASH),
                                       new KeySchemaElement(MTAmazonDynamoDBTestRunner.rangeKeyField, KeyType.RANGE))
                        .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                                .withKeySchema(new KeySchemaElement(MTAmazonDynamoDBTestRunner.indexField, KeyType.HASH))
                                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                        .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                                .withKeySchema(new KeySchemaElement(MTAmazonDynamoDBTestRunner.hashKeyField, KeyType.HASH),
                                               new KeySchemaElement(indexRangeField, KeyType.RANGE))
                                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
            } else {
                return new CreateTableRequest()
                        .withTableName(tableName)
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withAttributeDefinitions(new AttributeDefinition(MTAmazonDynamoDBTestRunner.hashKeyField, S))
                        .withKeySchema(new KeySchemaElement(MTAmazonDynamoDBTestRunner.hashKeyField, KeyType.HASH));
            }
        };
        run(() -> defaultSettings(SharedTableCustomDynamicBuilder.builder().withAmazonDynamoDB(amazonDynamoDB)
                                                                .withContext(mtContext)
                                                                .withCreateTableRequestFactory(createTableRequestFactory)
                                                                .withTruncateOnDeleteTable(true)).build());
    }

    @Test
    void sharedTableCustomStatic() {
        run(() -> defaultSettings(SharedTableCustomStaticBuilder.builder()
                .withCreateTableRequests(
                        new CreateTableRequest()
                            .withTableName(hkTableName)
                            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S))
                            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH)),
                        new CreateTableRequest()
                            .withTableName(hkRkTableName)
                            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                            .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S),
                                    new AttributeDefinition(rangeKeyField, S),
                                    new AttributeDefinition(indexField, S),
                                    new AttributeDefinition(indexRangeField, S))
                            .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH), new KeySchemaElement(rangeKeyField, KeyType.RANGE))
                            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                                    .withKeySchema(new KeySchemaElement(indexField, KeyType.HASH))
                                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                            .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                                    .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                            new KeySchemaElement(indexRangeField, KeyType.RANGE))
                                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL))))
                .withTableMapper(virtualTableDescription ->
                        virtualTableDescription.getTableName().endsWith("3") ? hkRkTableName : hkTableName)
                .withAmazonDynamoDB(amazonDynamoDB)
                .withContext(mtContext)
                .withTruncateOnDeleteTable(true)).build());
    }

    /*
     * Tests the builder, which maps all requests to two tables, one with a string hashkey and one with a string
     * hashkey and rangekey.
     */
    @Test
    void sharedTable() {
        run(() -> defaultSettings(SharedTableBuilder.builder()
                .withAmazonDynamoDB(amazonDynamoDB)
                .withContext(mtContext)
                .withTruncateOnDeleteTable(true)).build());
    }

    /*
     * Each test run needs its own AmazonDynamoDB because table mappings are cached per instance and running
     * consecutive tests using the same table names with different primary key types trigger makes the caches invalid.
     */
    private void run(Supplier<AmazonDynamoDB> amazonDynamoDBSupplier) {
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                amazonDynamoDBSupplier.get(),
                rootAmazonDynamoDB,
                isLocalDynamo, S).runAll();
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                amazonDynamoDBSupplier.get(),
                rootAmazonDynamoDB,
                isLocalDynamo, N).runAll();
        new MTAmazonDynamoDBTestRunner(
                mtContext,
                amazonDynamoDBSupplier.get(),
                rootAmazonDynamoDB,
                isLocalDynamo, S).runBinaryTest();
    }

    private static String random(String fieldName) {
        return fieldName + (randomFieldNames ? randomUUID().toString().replace("-", "") : "");
    }

    private SharedTableCustomDynamicBuilder defaultSettings(SharedTableCustomDynamicBuilder sharedTableBuilder) {
        sharedTableBuilder.withTablePrefix(getPrefix());
        sharedTableBuilder.withPollIntervalSeconds((isLocalDynamo ? 0 : 5));
        return sharedTableBuilder;
    }

    private String getPrefix() {
        return isLocalDynamo
                ? ""
                : "oktodelete-" +
                  TestAmazonDynamoDBAdminUtils.getLocalHost() +
                  "-" + (randomTableName ? randomUUID() + "-" : "");
    }

}