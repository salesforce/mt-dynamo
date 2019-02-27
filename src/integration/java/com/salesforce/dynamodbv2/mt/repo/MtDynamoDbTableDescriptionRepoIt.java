package com.salesforce.dynamodbv2.mt.repo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtDynamoDbTableDescriptionRepoIt {

    /* This tests against hosted DynamoDB to ensure Billing Mode PPR is set (not supported in local dynamo) */

    static final Regions REGION = Regions.US_EAST_1;
    private static AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();
    private static AmazonDynamoDbAdminUtils remoteUtils = new AmazonDynamoDbAdminUtils(remoteDynamoDB);
    private static final Optional<String> TABLE_PREFIX = Optional.of("oktodelete-testBillingMode.");
    String tableName;
    String fullTableName;

    void createTableInMtDynamoDbTableDescriptionRepo(
            MtAmazonDynamoDbContextProvider ctx,
            MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder b) {

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
                repo.createTable(new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(new KeySchemaElement("id", KeyType.HASH)))
        );
    }

    void assertPayPerRequestIsSet() {
        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(
                fullTableName).getTable().getBillingModeSummary().getBillingMode());
    }

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = TABLE_PREFIX.get() + tableName;
    }

    @Test
    void testMtDynamoDbTableDescriptionPayPerRequestIsSet() throws InterruptedException {
        MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder b =
                MtDynamoDbTableDescriptionRepo.builder()
                        .withBillingMode(BillingMode.PAY_PER_REQUEST)
                        .withAmazonDynamoDb(remoteDynamoDB)
                        .withContext(ctx)
                        .withTablePrefix(TABLE_PREFIX)
                        .withTableDescriptionTableName(tableName);

        createTableInMtDynamoDbTableDescriptionRepo(ctx, b);
        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);
        assertPayPerRequestIsSet();
    }

    // PPR should take precedence over set ProvisionedThroughput
    @Test
    void testMtDynamoDbTableDescriptionPayPerRequestIsSetIfProvisionedThroughputIsAlsoSet()
            throws InterruptedException {
        MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder b =
                MtDynamoDbTableDescriptionRepo.builder()
                        .withBillingMode(BillingMode.PAY_PER_REQUEST)
                        .withProvisionedThroughput(5L)
                        .withAmazonDynamoDb(remoteDynamoDB)
                        .withContext(ctx)
                        .withTablePrefix(TABLE_PREFIX)
                        .withTableDescriptionTableName(tableName);

        createTableInMtDynamoDbTableDescriptionRepo(ctx, b);
        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);
        assertPayPerRequestIsSet();
    }

    @AfterEach
    void afterEach() {
        remoteUtils.deleteTableIfExists(fullTableName, 10, 600);
    }
}