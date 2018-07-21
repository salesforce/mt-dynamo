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

    private static final boolean randomTableName = false;
    private static final Regions region = Regions.US_EAST_1;
    /*
     * To run against hosted DynamoDB, you need AWS credentials in your .aws/credentials file,
     * see https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html.
     */
    private static final boolean isLocalDynamo = true;
    private static final AmazonDynamoDB rootAmazonDynamoDb = isLocalDynamo
        ? MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDb()
        : AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    private static final AmazonDynamoDBStreams rootAmazonDynamoDbStreams = isLocalDynamo
        ? MtAmazonDynamoDbTestRunner.getLocalAmazonDynamoDbStreams()
        : AmazonDynamoDBStreamsClientBuilder.standard().withRegion(region).build();
    private static final AWSCredentialsProvider awsCredentialsProvider = isLocalDynamo
        ? new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
        : new DefaultAWSCredentialsProviderChain();
    private static final MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();
    private static final AmazonDynamoDB amazonDynamoDb = MtAmazonDynamoDbLogger.builder()
        .withAmazonDynamoDb(rootAmazonDynamoDb)
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
                    .withAttributeDefinitions(new AttributeDefinition(MtAmazonDynamoDbTestRunner.hashKeyField, S),
                        new AttributeDefinition(MtAmazonDynamoDbTestRunner.rangeKeyField, S),
                        new AttributeDefinition(MtAmazonDynamoDbTestRunner.indexField, S),
                        new AttributeDefinition(indexRangeField, S))
                    .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.hashKeyField, KeyType.HASH),
                        new KeySchemaElement(MtAmazonDynamoDbTestRunner.rangeKeyField, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.indexField, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(MtAmazonDynamoDbTestRunner.hashKeyField, KeyType.HASH),
                            new KeySchemaElement(indexRangeField, KeyType.RANGE))
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
        run(() -> defaultSettings(SharedTableCustomDynamicBuilder.builder().withAmazonDynamoDb(amazonDynamoDb)
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
                    .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)),
                new CreateTableRequest()
                    .withTableName(hkRkTableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S),
                        new AttributeDefinition(rangeKeyField, S),
                        new AttributeDefinition(indexField, S),
                        new AttributeDefinition(indexRangeField, S))
                    .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                            new KeySchemaElement(rangeKeyField, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(indexField, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                            new KeySchemaElement(indexRangeField, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)))
            .withTableMapper(virtualTableDescription ->
                virtualTableDescription.getTableName().endsWith("3") ? hkRkTableName : hkTableName)
            .withAmazonDynamoDb(amazonDynamoDb)
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
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(mtContext)
            .withTruncateOnDeleteTable(true)).build());
    }

    /*
     * Each test run needs its own AmazonDynamoDB because table mappings are cached per instance and running
     * consecutive tests using the same table names with different primary key types trigger makes the caches invalid.
     */
    private void run(Supplier<AmazonDynamoDB> amazonDynamoDbSupplier) {
        new MtAmazonDynamoDbTestRunner(
            mtContext,
            amazonDynamoDbSupplier.get(),
            rootAmazonDynamoDb,
            rootAmazonDynamoDbStreams, // test streams for this run only
            awsCredentialsProvider,
            isLocalDynamo, S).runAll();
        new MtAmazonDynamoDbTestRunner(
            mtContext,
            amazonDynamoDbSupplier.get(),
            rootAmazonDynamoDb,
            null,
            awsCredentialsProvider,
            isLocalDynamo, N).runAll();
        new MtAmazonDynamoDbTestRunner(
            mtContext,
            amazonDynamoDbSupplier.get(),
            rootAmazonDynamoDb,
            null,
            awsCredentialsProvider,
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
        if (isLocalDynamo) {
            return "";
        } else {
            return "oktodelete-"
                    + TestAmazonDynamoDbAdminUtils.getLocalHost()
                    + "-"
                    + (randomTableName ? randomUUID() + "-" : "");
        }
    }

}