package com.salesforce.dynamodbv2.mt.repo;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.MT_VIRTUAL_TABLE_PREFIX;
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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataRequest;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.ListMetadataResult;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo.MtTenantTableDesciption;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtDynamoDbTableDescriptionRepoTest {

    private static final String GLOBAL_CONTEXT_PLACEHOLDER = "testGlobalPlaceholder";

    private final AmazonDynamoDB localDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private String descriptionTableName;
    private MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder mtDynamoDbTableDescriptionRepoBuilder;

    @BeforeEach
    void beforeEach() {
        descriptionTableName = getClass().getSimpleName() + "." + System.currentTimeMillis();
        mtDynamoDbTableDescriptionRepoBuilder = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(localDynamoDb)
            .withContext(MT_CONTEXT)
            .withGlobalContextPlaceholder(GLOBAL_CONTEXT_PLACEHOLDER)
            .withMultitenantVirtualTableCheck(t -> t.startsWith(MT_VIRTUAL_TABLE_PREFIX))
            .withTableDescriptionTableName(descriptionTableName)
            .withPollIntervalSeconds(0);
    }

    /**
     * Verifies that changing provisioned throughput on the metadata table does not cause comparison to fail on restart.
     */
    @Test
    void testMetadataTableProvisioningThroughputChange() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        MT_CONTEXT.withContext("1", () ->
            repo.createTableMetadata(new CreateTableRequest()
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)), false)
        );

        localDynamoDb.updateTable(new UpdateTableRequest(descriptionTableName, new ProvisionedThroughput(
            6L, 6L)));

        MtDynamoDbTableDescriptionRepo repo2 = mtDynamoDbTableDescriptionRepoBuilder.build();
        try {
            MT_CONTEXT.withContext("1", () -> repo2.getTableDescription("test"));
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
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)), false)
        );

        TableUtils.waitUntilActive(localDynamoDb, descriptionTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(descriptionTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtDynamoDbTableDescriptionPayPerRequestIsSet() throws InterruptedException {
        mtDynamoDbTableDescriptionRepoBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        MT_CONTEXT.withContext("1", () ->
            repo.createTableMetadata(new CreateTableRequest()
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH)), false)
        );

        TableUtils.waitUntilActive(localDynamoDb, descriptionTableName);
        DynamoDbTestUtils.assertPayPerRequestIsSet(descriptionTableName, localDynamoDb);
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

    @Test
    void testGetTableDescription() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        Set<MtTenantTableDesciption> tablesCreated = createVirtualTables(repo);
        MT_CONTEXT.withContext("1", () -> {
            for (MtTenantTableDesciption table : tablesCreated) {
                if (table.getTenantName().equals("1") || table.getTableDescription().isMultitenant()) {
                    assertEquals(table.getTableDescription(),
                        repo.getTableDescription(table.getTableDescription().getTableName()));
                } else {
                    try {
                        repo.getTableDescription(table.getTableDescription().getTableName());
                        fail("Should not be able to get table of another tenant");
                    } catch (ResourceNotFoundException e) {
                        // expected
                    }
                }
            }
        });
    }

    @Test
    void testGetTableDescription_empty() {
        MtDynamoDbTableDescriptionRepo repo = mtDynamoDbTableDescriptionRepoBuilder.build();
        Set<MtTenantTableDesciption> tablesCreated = createVirtualTables(repo);
        MT_CONTEXT.withContext(null, () -> {
            for (MtTenantTableDesciption table : tablesCreated) {
                if (table.getTableDescription().isMultitenant()) {
                    assertEquals(table.getTableDescription(),
                        repo.getTableDescription(table.getTableDescription().getTableName()));
                } else {
                    try {
                        repo.getTableDescription(table.getTableDescription().getTableName());
                        fail("Should not be able to get single-tenant table when there is no context");
                    } catch (ResourceNotFoundException e) {
                        // expected
                    }
                }
            }
        });
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
        CreateTableRequest mtCreateReq = new CreateTableRequest()
            .withTableName(MT_VIRTUAL_TABLE_PREFIX + tablePrefix + "3")
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH));

        MtTableDescription table1 = MT_CONTEXT.withContext(tenant1, () -> repo.createTableMetadata(createReq1, false));
        MtTableDescription table2 = MT_CONTEXT.withContext(tenant2, () -> repo.createTableMetadata(createReq2, false));
        MtTableDescription mtTable = MT_CONTEXT.withContext(null, () -> repo.createTableMetadata(mtCreateReq, true));
        assertEquals(MtDynamoDbTableDescriptionRepo.toTableDescription(createReq1, false), table1);
        assertEquals(MtDynamoDbTableDescriptionRepo.toTableDescription(createReq2, false), table2);
        assertEquals(MtDynamoDbTableDescriptionRepo.toTableDescription(mtCreateReq, true), mtTable);

        return ImmutableSet.of(
            new MtTenantTableDesciption(tenant1, table1),
            new MtTenantTableDesciption(tenant2, table2),
            new MtTenantTableDesciption(GLOBAL_CONTEXT_PLACEHOLDER, mtTable));
    }
}
