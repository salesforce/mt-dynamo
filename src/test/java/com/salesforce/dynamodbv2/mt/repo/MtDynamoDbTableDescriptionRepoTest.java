package com.salesforce.dynamodbv2.mt.repo;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.GLOBAL_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.SharedTableNamingRules;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataRequest;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataResult;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.MtTenantTableDesciption;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtDynamoDbTableDescriptionRepoTest {

    private final AmazonDynamoDB localDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private String tableName;
    private String fullTableName;

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
        new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final Optional<String> tablePrefix = Optional.of("MtDynamoDbTableDescriptionRepoTest.");
    private MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder mtDynamoDbTableDescriptionRepoBuilder;

    @BeforeEach
    void beforeEach() {
        tableName = String.valueOf(System.currentTimeMillis());
        fullTableName = DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix.orElseThrow(), tableName, "");

        mtDynamoDbTableDescriptionRepoBuilder = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(localDynamoDb)
            .withContext(MT_CONTEXT)
            .withGlobalContext(Optional.of(GLOBAL_CONTEXT))
            .withTablePrefix(tablePrefix)
            .withTableDescriptionTableName(tableName)
            .withPollIntervalSeconds(0);
    }

    /**
     * Verifies that changing provisioned throughput on the metadata table does not cause comparison to fail on restart.
     */
    @Test
    void testMetadataTableProvisioningThroughputChange() {
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        String tableName = "MtDynamoDbTableDescriptionRepoTest_testMetadataTableExists_metadata";

        MtDynamoDbTableDescriptionRepoBuilder b = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(dynamoDb)
            .withContext(ctx)
            .withTableDescriptionTableName(tableName)
            .withPollIntervalSeconds(0);

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
            repo.createTableMetadata(new CreateTableRequest()
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)), false)
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
            repo.createTableMetadata(new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)), false)
        );

        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(fullTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtDynamoDbTableDescriptionPayPerRequestIsSet() throws InterruptedException {
        mtDynamoDbTableDescriptionRepoBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        MT_CONTEXT.withContext("1", () ->
            repo.createTableMetadata(new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)), false)
        );

        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertPayPerRequestIsSet(fullTableName, localDynamoDb);
    }

    @Test
    void testListVirtualTables() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        Set<MtTenantTableDesciption> tablesCreated = createVirtualTables(repo);
        ListMetadataResult listMetadataResult = repo.listVirtualTableMetadata(new ListMetadataRequest());
        assertEquals(tablesCreated, new HashSet<>(listMetadataResult.getTenantTableDescriptions()));
        assertNull(listMetadataResult.getLastEvaluatedTable());
    }

    @Test
    void testListVirtualTables_pagination() {
        final MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        final Set<MtTenantTableDesciption> tablesCreated = createVirtualTables(repo);
        final Set<MtTenantTableDesciption> tablesReturned = new HashSet<>();

        ListMetadataResult listMetadataResult = repo.listVirtualTableMetadata(new ListMetadataRequest().withLimit(1));
        assertEquals(1, listMetadataResult.getTenantTableDescriptions().size());
        assertNotNull(listMetadataResult.getLastEvaluatedTable());
        tablesReturned.add(listMetadataResult.getLastEvaluatedTable());

        listMetadataResult = repo.listVirtualTableMetadata(new ListMetadataRequest().withLimit(5)
            .withExclusiveStartCreateTableReq(listMetadataResult.getLastEvaluatedTable()));
        assertEquals(2, listMetadataResult.getTenantTableDescriptions().size());
        assertNull(listMetadataResult.getLastEvaluatedTable());
        tablesReturned.addAll(listMetadataResult.getTenantTableDescriptions());

        assertEquals(tablesCreated, tablesReturned);
    }

    @Test
    void testListTables() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        Set<String> tablesCreated = createVirtualTables(repo).stream()
            .filter(t -> t.getTenantName().equals("1") || t.getTableDescription().isMultitenant())
            .map(t -> t.getTableDescription().getTableName())
            .collect(Collectors.toSet());
        MT_CONTEXT.withContext("1", () -> {
            ListTablesResult listTablesResult =
                ((MtTableDescriptionRepo) repo).listTables(new ListTablesRequest());
            assertEquals(tablesCreated, new HashSet<>(listTablesResult.getTableNames()));
        });
    }

    @Test
    void testListTables_pagination() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        Set<MtTenantTableDesciption> tablesCreated = new HashSet<>();
        tablesCreated.addAll(createVirtualTables(repo, "table1"));
        tablesCreated.addAll(createVirtualTables(repo, "table2"));
        tablesCreated.addAll(createVirtualTables(repo, "table3"));

        Set<String> expectedReturnedTables = tablesCreated.stream()
            .filter(t -> t.getTenantName().equals("1") || t.getTableDescription().isMultitenant())
            .map(t -> t.getTableDescription().getTableName())
            .collect(Collectors.toSet());
        assertEquals(6, expectedReturnedTables.size());

        ListTablesRequest listTablesRequest = new ListTablesRequest().withLimit(1);
        MT_CONTEXT.withContext("1", () -> {
            int numScans = 0;
            do {
                ListTablesResult listTablesResult = repo.listTables(listTablesRequest);
                listTablesRequest.withExclusiveStartTableName(listTablesResult.getLastEvaluatedTableName());
                if (numScans < 6) {
                    assertEquals(1, listTablesResult.getTableNames().size());
                    assertTrue(expectedReturnedTables.remove(listTablesResult.getTableNames().get(0)),
                        "did not remove: " + listTablesResult.getTableNames().get(0));
                } else {
                    assertTrue(listTablesResult.getTableNames().isEmpty());
                }
                numScans++;
            } while (listTablesRequest.getExclusiveStartTableName() != null);
            assertTrue(expectedReturnedTables.isEmpty(), "Expected all created tables to be returned by scan");
        });

    }

    @Test
    void testListVirtualTables_empty() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        ListMetadataResult listMetadataResult = repo.listVirtualTableMetadata(new ListMetadataRequest());
        assertEquals(new ListMetadataResult(ImmutableList.of(), null), listMetadataResult);
    }

    private Set<MtTenantTableDesciption> createVirtualTables(MtDynamoDbTableDescriptionRepo repo) {
        return createVirtualTables(repo, "table");
    }

    private Set<MtTenantTableDesciption> createVirtualTables(
        MtDynamoDbTableDescriptionRepo repo, String tablePrefix) {
        String tenant1 = "1";
        String tenant2 = "2";
        String table1Gsi = "index";
        String tableName1 = tablePrefix + "1";
        String tableName2 = tablePrefix + "2";
        CreateTableRequest createReq1 = new CreateTableRequest()
            .withTableName(tableName1)
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                .withIndexName(table1Gsi)
                .withKeySchema(new KeySchemaElement("secondary-id", KeyType.HASH)));
        CreateTableRequest createReq2 = new CreateTableRequest()
            .withTableName(tableName2)
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH));
        CreateTableRequest mtTableCreateReq = new CreateTableRequest()
            .withTableName(SharedTableNamingRules.VIRTUAL_MULTITENANT_TABLE_PREFIX + tablePrefix + "3")
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH));
        MT_CONTEXT.withContext(tenant1, () ->
            repo.createTableMetadata(createReq1, false));
        MT_CONTEXT.withContext(tenant2, () ->
            repo.createTableMetadata(createReq2, false));
        MT_CONTEXT.withContext(GLOBAL_CONTEXT, () ->
            repo.createTableMetadata(mtTableCreateReq, true));

        return ImmutableSet.of(
            new MtTenantTableDesciption(tenant1, MtDynamoDbTableDescriptionRepo.toTableDescription(createReq1, false)),
            new MtTenantTableDesciption(tenant2, MtDynamoDbTableDescriptionRepo.toTableDescription(createReq2, false)),
            new MtTenantTableDesciption(GLOBAL_CONTEXT,
                MtDynamoDbTableDescriptionRepo.toTableDescription(mtTableCreateReq, true)));
    }
}
