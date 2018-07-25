/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static java.util.UUID.randomUUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.TestAmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomDynamicBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomStaticBuilder;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class MtAmazonDynamoDbBySharedTableTest {

    private static final boolean RANDOM_TABLE_NAME = false;
    private static final Regions REGION = Regions.US_EAST_1;
    /*
     * To run against hosted DynamoDB, you need AWS credentials in your .aws/credentials file,
     * see https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html.
     */
    private static final boolean IS_LOCAL_DYNAMO = true;
    private static final AmazonDynamoDB ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDb()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    private static final AmazonDynamoDBStreams ROOT_AMAZON_DYNAMO_DB_STREAMS = IS_LOCAL_DYNAMO
        ? MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDbStreams()
        : AmazonDynamoDBStreamsClientBuilder.standard().withRegion(REGION).build();
    private static final AWSCredentialsProvider AWS_CREDENTIALS_PROVIDER = IS_LOCAL_DYNAMO
        ? new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
        : new DefaultAWSCredentialsProviderChain();
    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = new MtAmazonDynamoDbContextProviderImpl();
    private static final AmazonDynamoDB AMAZON_DYNAMO_DB = MtAmazonDynamoDbLogger.builder()
        .withAmazonDynamoDb(ROOT_AMAZON_DYNAMO_DB)
        .withContext(MT_CONTEXT)
        .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
            "putItem", "query", "scan", "updateItem")).build();
    private static final boolean RANDOM_FIELD_NAMES = true;
    private static final String HK_TABLE_NAME = "hkTable";
    private static final String HK_RK_TABLE_NAME = "hkRkTable";
    private static final String HASH_KEY_FIELD = random("HASH_KEY_FIELD");
    private static final String RANGE_KEY_FIELD = random("RANGE_KEY_FIELD");
    private static final String INDEX_FIELD = random("INDEX_FIELD");
    private static final String INDEX_RANGE_FIELD = random("INDEX_RANGE_FIELD");

    @Test
    void sharedTableCustomDynamic() {
        CreateTableRequestFactory createTableRequestFactory = virtualTableDescription -> {
            String tableName = virtualTableDescription.getTableName();
            if (tableName.endsWith("3")) {
                return new CreateTableRequest()
                    .withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(MtAmazonDynamoDbTestRunner.hashKeyField, S),
                        new AttributeDefinition(MtAmazonDynamoDbTestRunner.rangeKeyField, S),
                        new AttributeDefinition(MtAmazonDynamoDbTestRunner.indexField, S),
                        new AttributeDefinition(INDEX_RANGE_FIELD, S))
                    .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.hashKeyField, KeyType.HASH),
                        new KeySchemaElement(MtAmazonDynamoDbTestRunner.rangeKeyField, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.indexField, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.hashKeyField, KeyType.HASH),
                            new KeySchemaElement(INDEX_RANGE_FIELD, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true));
            } else {
                return new CreateTableRequest()
                    .withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(MtAmazonDynamoDbTestRunner.hashKeyField, S))
                    .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.hashKeyField, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true));
            }
        };
        run(() -> defaultSettings(SharedTableCustomDynamicBuilder.builder().withAmazonDynamoDb(AMAZON_DYNAMO_DB)
            .withContext(MT_CONTEXT)
            .withCreateTableRequestFactory(createTableRequestFactory)
            .withTruncateOnDeleteTable(true)).build());
    }

    @Test
    void sharedTableCustomStatic() {
        run(() -> defaultSettings(SharedTableCustomStaticBuilder.builder()
            .withCreateTableRequests(
                new CreateTableRequest()
                    .withTableName(HK_TABLE_NAME)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)),
                new CreateTableRequest()
                    .withTableName(HK_RK_TABLE_NAME)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                        new AttributeDefinition(RANGE_KEY_FIELD, S),
                        new AttributeDefinition(INDEX_FIELD, S),
                        new AttributeDefinition(INDEX_RANGE_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(INDEX_RANGE_FIELD, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)))
            .withTableMapper(virtualTableDescription ->
                virtualTableDescription.getTableName().endsWith("3") ? HK_RK_TABLE_NAME : HK_TABLE_NAME)
            .withAmazonDynamoDb(AMAZON_DYNAMO_DB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true)).build());
    }

    /*
     * Tests the builder, which maps all requests to two tables, one with a string hashkey and one with a string
     * hashkey and rangekey.
     */
    @Test
    void sharedTable() {
        run(() -> defaultSettings(SharedTableBuilder.builder()
            .withAmazonDynamoDb(AMAZON_DYNAMO_DB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true)).build());
    }

    /*
     * Each test run needs its own AmazonDynamoDB because table mappings are cached per instance and running
     * consecutive tests using the same table names with different primary key types trigger makes the caches invalid.
     */
    private void run(Supplier<AmazonDynamoDB> amazonDynamoDbSupplier) {
        new MtAmazonDynamoDbTestRunner(
                MT_CONTEXT,
            amazonDynamoDbSupplier.get(),
                ROOT_AMAZON_DYNAMO_DB,
                ROOT_AMAZON_DYNAMO_DB_STREAMS, // test streams for this run only
                AWS_CREDENTIALS_PROVIDER,
                IS_LOCAL_DYNAMO, S).runAll();
        new MtAmazonDynamoDbTestRunner(
                MT_CONTEXT,
            amazonDynamoDbSupplier.get(),
                ROOT_AMAZON_DYNAMO_DB,
            null,
                AWS_CREDENTIALS_PROVIDER,
                IS_LOCAL_DYNAMO, N).runAll();
        new MtAmazonDynamoDbTestRunner(
                MT_CONTEXT,
            amazonDynamoDbSupplier.get(),
                ROOT_AMAZON_DYNAMO_DB,
            null,
                AWS_CREDENTIALS_PROVIDER,
                IS_LOCAL_DYNAMO, S).runBinaryTest();
    }

    private static String random(String fieldName) {
        return fieldName + (RANDOM_FIELD_NAMES ? randomUUID().toString().replace("-", "") : "");
    }

    private SharedTableCustomDynamicBuilder defaultSettings(SharedTableCustomDynamicBuilder sharedTableBuilder) {
        sharedTableBuilder.withTablePrefix(getPrefix());
        sharedTableBuilder.withPollIntervalSeconds((IS_LOCAL_DYNAMO ? 0 : 5));
        return sharedTableBuilder;
    }

    private String getPrefix() {
        if (IS_LOCAL_DYNAMO) {
            return "";
        } else {
            return "oktodelete-"
                    + TestAmazonDynamoDbAdminUtils.getLocalHost()
                    + "-"
                    + (RANDOM_TABLE_NAME ? randomUUID() + "-" : "");
        }
    }

}