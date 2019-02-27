package com.salesforce.dynamodbv2.mt.repo;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MtDynamoDbTableDescriptionRepoTest {

    private final Optional<String> tablePrefix = Optional.of("oktodelete-testBillingMode.");
    private String tableName;
    private String fullTableName;
    private AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = tablePrefix.get() + tableName;
    }

    /**
     * Verifies provisioned throughput is set.
     */
    public void assertProvisionedThroughputIsSet(Long expectedThroughput) throws InterruptedException {
        TableUtils.waitUntilActive(dynamoDb, fullTableName);

        assertNotNull(dynamoDb.describeTable(fullTableName).getTable());
        assertNotNull(dynamoDb.describeTable(fullTableName).getTable().getProvisionedThroughput());

        assert (dynamoDb.describeTable(fullTableName).getTable().getProvisionedThroughput()
                .getReadCapacityUnits().equals(expectedThroughput));
        assert (dynamoDb.describeTable(fullTableName).getTable().getProvisionedThroughput()
                .getWriteCapacityUnits().equals(expectedThroughput));
    }

    /**
     * Verifies that changing provisioned throughput on the metadata table doesn't cause comparison to fail on restart.
     */
    @Test
    void testMetadataTableProvisioningThroughputChange() {
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        String tableName = "MtDynamoDbTableDescriptionRepoTest_testMetadataTableExists_metadata";

        MtDynamoDbTableDescriptionRepoBuilder b = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(dynamoDb)
            .withContext(ctx)
            .withTableDescriptionTableName(tableName);

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
            repo.createTable(new CreateTableRequest()
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)))
        );

        dynamoDb.updateTable(new UpdateTableRequest(tableName, new ProvisionedThroughput(
                6L, 6L)));

        MtDynamoDbTableDescriptionRepo repo2 = b.build();
        try {
            ctx.withContext("1", () -> repo2.getTableDescription("test"));
            // if no exception was thrown, the repo properly initialized using the existing metadata table
        } catch (Exception e) {
            // otherwise, check which exception was thrown to distinguish between test failure and error
            Throwable cause = e;
            while ((cause = cause.getCause()) != null) {
                if (cause instanceof IllegalArgumentException && cause.getMessage().contains("table does not match")) {
                    fail("Description repo rejected metadata table after provisioned throughput change.", e);
                }
            }
            throw e;
        }
    }

    /**
     * Verifies that setting the throughput to non-zero value sets provisioned throughput.
     */
    @Test
    void testMtDynamoDbTableDescriptionProvisionedThroughputIsSetWhenSet() throws InterruptedException {

        MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder b =
                MtDynamoDbTableDescriptionRepo.builder()
                        .withAmazonDynamoDb(dynamoDb)
                        .withContext(ctx)
                        .withTablePrefix(tablePrefix)
                        .withTableDescriptionTableName(tableName)
                        .withProvisionedThroughput(5L);

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
                repo.createTable(new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(new KeySchemaElement("id", KeyType.HASH)))
        );

        assertProvisionedThroughputIsSet(5L);
    }

    /**
     * Verifies not setting throughput, sets provisioned throughput to defaults.
     */
    @Test
    void testMtDynamoDbTableDescriptionProvisionedThroughputIsSetWhenDefault()  throws InterruptedException {

        MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder b =
                MtDynamoDbTableDescriptionRepo.builder()
                        .withAmazonDynamoDb(dynamoDb)
                        .withContext(ctx)
                        .withTablePrefix(tablePrefix)
                        .withTableDescriptionTableName(tableName);

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
                repo.createTable(new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(new KeySchemaElement("id", KeyType.HASH)))
        );

        assertProvisionedThroughputIsSet(1L);
    }
}
