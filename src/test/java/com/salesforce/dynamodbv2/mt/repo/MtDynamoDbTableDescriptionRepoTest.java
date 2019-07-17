package com.salesforce.dynamodbv2.mt.repo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataRequest;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataResult;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.MtCreateTableRequest;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtDynamoDbTableDescriptionRepoTest {

    private final AmazonDynamoDB localDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private String tableName;
    private String fullTableName;

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
        new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final Optional<String> tablePrefix = Optional.of("okToDelete-testBillingMode.");
    private MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder mtDynamoDbTableDescriptionRepoBuilder;

    @BeforeEach
    void beforeEach() {
        tableName = String.valueOf(System.currentTimeMillis());
        fullTableName = DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix.orElseThrow(), tableName, "");

        mtDynamoDbTableDescriptionRepoBuilder = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(localDynamoDb)
            .withContext(MT_CONTEXT)
            .withTablePrefix(tablePrefix)
            .withTableDescriptionTableName(tableName);
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
     * Verifies not setting throughput, sets provisioned throughput to defaults.
     */
    @Test
    void testMtDynamoDbTableDescriptionProvisionedThroughputIsSetWhenDefault() throws InterruptedException {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        MT_CONTEXT.withContext("1", () ->
            repo.createTable(new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)))
        );

        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(fullTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtDynamoDbTableDescriptionPayPerRequestIsSet() throws InterruptedException {
        mtDynamoDbTableDescriptionRepoBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        MT_CONTEXT.withContext("1", () ->
            repo.createTable(new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)))
        );

        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertPayPerRequestIsSet(fullTableName, localDynamoDb);
    }

    @Test
    void testListVirtualTables() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        List<MtCreateTableRequest> tablesCreated = createPairOfVirtualTables(repo);
        ListMetadataResult returnedMetadatas =
            ((MtTableDescriptionRepo)repo).listVirtualTableMetadata(new ListMetadataRequest());
        assertEquals(new ListMetadataResult(tablesCreated, null), returnedMetadatas);
    }

    @Test
    void testListVirtualTables_pagination() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        List<MtCreateTableRequest> tablesCreated = createPairOfVirtualTables(repo);
        ListMetadataResult returnedMetadatas = repo.listVirtualTableMetadata(new ListMetadataRequest().withLimit(1));
        ListMetadataResult expected = new ListMetadataResult(tablesCreated.subList(0,1), tablesCreated.get(0));
        assertEquals(expected, returnedMetadatas);

        returnedMetadatas =
            repo.listVirtualTableMetadata(new ListMetadataRequest().withExclusiveStartKey(tablesCreated.get(0)));
        assertEquals(returnedMetadatas,
            repo.listVirtualTableMetadata(new ListMetadataRequest()
                .withExclusiveStartKey(tablesCreated.get(0))
                .withLimit(5)));
        expected = new ListMetadataResult(ImmutableList.of(tablesCreated.get(1)), null);
        assertEquals(expected,
            repo.listVirtualTableMetadata(new ListMetadataRequest().withExclusiveStartKey(tablesCreated.get(0))));
    }

    @Test
    void testListVirtualTables_empty() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        ListMetadataResult returnedMetadatas = repo.listVirtualTableMetadata(new ListMetadataRequest());
        assertEquals(new ListMetadataResult(ImmutableList.of(), null), returnedMetadatas);
    }

    private List<MtCreateTableRequest> createPairOfVirtualTables(MtDynamoDbTableDescriptionRepo repo) {
        String tenant1 = "1";
        String tenant2 = "2";
        String table1Gsi = "index";
        String tableName1 = "table";
        String tableName2 = "table2";
        CreateTableRequest createReq1 = new CreateTableRequest()
            .withTableName(tableName1)
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                .withIndexName(table1Gsi)
                .withKeySchema(new KeySchemaElement("secondary-id", KeyType.HASH)));
        CreateTableRequest createReq2 = new CreateTableRequest()
            .withTableName(tableName2)
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH));
        MT_CONTEXT.withContext(tenant1, () ->
            repo.createTable(createReq1));
        MT_CONTEXT.withContext(tenant2, () ->
            repo.createTable(createReq2));

        return ImmutableList.of(new MtCreateTableRequest(tenant1, createReq1),
            new MtCreateTableRequest(tenant2, createReq2));
    }
}
