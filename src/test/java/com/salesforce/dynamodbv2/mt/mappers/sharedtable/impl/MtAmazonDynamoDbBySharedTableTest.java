package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.backups.MtScanningSnapshotter;
import com.salesforce.dynamodbv2.mt.backups.SnapshotRequest;
import com.salesforce.dynamodbv2.mt.backups.SnapshotResult;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultTestSetup;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Unit tests for specific MtAmazonDynamoDbBySharedTable methods.
 *
 * @author msgroi
 */
class MtAmazonDynamoDbBySharedTableTest {

    private static final String HASH_KEY = "hashKey";

    @Test
    void projectionContainsKey_nullProject() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(new ScanRequest(), null));
    }

    @Test
    void projectionContainsKey_hkInProjection() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withProjectionExpression("hk"), new PrimaryKey("hk", S)));
    }

    @Test
    void projectionContainsKey_hkInExpressionAttrNames() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withProjectionExpression("value")
                .withExpressionAttributeNames(ImmutableMap.of("hk", "value")),
            new PrimaryKey("hk", S)));
    }

    @Test
    void projectionContainsKey_hkInLegacyProjection() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withAttributesToGet("hk"), new PrimaryKey("hk", S)));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(SharedTableArgumentProvider.class)
    public void testListTables_noContext(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        final List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();

        assertFalse(allTables.isEmpty());
        MtScanningSnapshotter tableSnapshotter = new MtScanningSnapshotter();
        List<SnapshotResult> snapshots = new ArrayList<>();
        try {
            for (String table : allTables) {
                snapshots.add(tableSnapshotter.snapshotTableToTarget(new SnapshotRequest("fake-backup",
                    table,
                    ((MtAmazonDynamoDbBySharedTable) testArgument.getAmazonDynamoDb()).getBackupTablePrefix() + table,
                    testArgument.getRootAmazonDynamoDb(),
                    new ProvisionedThroughput(10L, 10L))));
            }
            assertEquals(allTables, testArgument.getAmazonDynamoDb().listTables().getTableNames());
        } finally {
            for (SnapshotResult snapshot : snapshots) {
                tableSnapshotter.cleanup(snapshot, testArgument.getRootAmazonDynamoDb());
            }
        }
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(ListVirtualTableProvider.class)
    public void testListTables_withContext(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            ListTablesResult listTablesResult = testArgument.getAmazonDynamoDb().listTables();
            assertEquals(TABLE_NAME_PREFIXES.size(), listTablesResult.getTableNames().size());
            for (String prefix : TABLE_NAME_PREFIXES) {
                assertTrue(listTablesResult.getTableNames().contains(prefix + org));
            }
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(ListVirtualTableProvider.class)
    public void testListTablesPagination_withContext(TestArgument testArgument) {
        testArgument.forEachOrgContext(org -> {
            String lastEvaluatedTable = null;
            List<String> tablesSeen = new ArrayList<>();
            int limit = 1;
            do {
                ListTablesResult listTablesResult = testArgument.getAmazonDynamoDb()
                    .listTables(lastEvaluatedTable, limit);
                if (tablesSeen.size() < TABLE_NAME_PREFIXES.size()) {
                    assertEquals(limit, listTablesResult.getTableNames().size());
                    tablesSeen.add(listTablesResult.getTableNames().get(0));
                } else {
                    assertEquals(0, listTablesResult.getTableNames().size());
                }
                lastEvaluatedTable = listTablesResult.getLastEvaluatedTableName();
            } while (lastEvaluatedTable != null);
            assertEquals(TABLE_NAME_PREFIXES.size(), tablesSeen.size());
            for (String prefix : TABLE_NAME_PREFIXES) {
                assertTrue(tablesSeen.contains(prefix + org));
            }
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(ListVirtualTableProvider.class)
    public void testPutItem_ScanColumnsReserved(TestArgument testArgument) {
        MtAmazonDynamoDbBySharedTable mtDynamo = (MtAmazonDynamoDbBySharedTable) testArgument.getAmazonDynamoDb();
        final String pk = "row1";
        // post some global data
        testArgument.forEachOrgContext(org -> {
            final List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();
            assertFalse(allTables.isEmpty());
            String arbitraryTable = allTables.get(0);

            List<String> reservedColumns = ImmutableList.of(mtDynamo.getMtScanTenantKey(),
                mtDynamo.getMtScanVirtualTableKey());
            for (String reservedColumn : reservedColumns) {
                Map<String, AttributeValue> row1 = ImmutableMap.of(HASH_KEY, new AttributeValue(pk),
                    reservedColumn, new AttributeValue("foo"));
                try {
                    PutItemRequest putItemRequest = new PutItemRequest().withTableName(arbitraryTable)
                        .withItem(row1);
                    mtDynamo.putItem(putItemRequest);
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("Trying to update a reserved column name"));
                }
            }
        });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(ListVirtualTableProvider.class)
    public void testUpdateItem_ScanColumnsReserved(TestArgument testArgument) {
        MtAmazonDynamoDbBySharedTable mtDynamo = (MtAmazonDynamoDbBySharedTable) testArgument.getAmazonDynamoDb();
        final String pk = "row1";
        // post some global data
        testArgument.forEachOrgContext(org -> {
            final List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();
            assertFalse(allTables.isEmpty());
            String arbitraryTable = allTables.get(0);


            Map<String, AttributeValue> row1 = ImmutableMap.of(HASH_KEY, new AttributeValue(pk));
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(arbitraryTable)
                .withItem(row1);
            mtDynamo.putItem(putItemRequest);
            List<String> reservedColumns = ImmutableList.of(
                mtDynamo.getMtScanTenantKey(),
                mtDynamo.getMtScanVirtualTableKey());

            for (String reservedColumn : reservedColumns) {
                UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                    .withTableName(arbitraryTable)
                    .withKey(row1)
                    .withUpdateExpression("set #someField = :someValue")
                    .withExpressionAttributeNames(ImmutableMap.of("#someField", reservedColumn))
                    .withExpressionAttributeValues(ImmutableMap.of(":someValue",
                        new AttributeValue("bar")));
                try {
                    mtDynamo.updateItem(updateItemRequest);
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("Trying to update a reserved column name"));
                }
            }
        });
    }

    private static class SharedTableArgumentProvider extends DefaultArgumentProvider {
        public SharedTableArgumentProvider() {
            super(new SharedTableArgumentBuilder(), new DefaultTestSetup(DefaultTestSetup.ALL_TABLES));
        }
    }

    private static final List<String> TABLE_NAME_PREFIXES = ImmutableList.of("table1-", "table2-", "table3-");

    private static class ListVirtualTableProvider extends DefaultArgumentProvider {
        public ListVirtualTableProvider() {
            super(new SharedTableArgumentBuilder(), new DefaultTestSetup(new String[0]) {

                @Override
                public void setupTest(TestArgument testArgument) {
                    CreateTableRequestBuilder baseBuilder = CreateTableRequestBuilder.builder()
                        .withAttributeDefinitions(
                            new AttributeDefinition("hashKey", ScalarAttributeType.S))
                        .withKeySchema(new KeySchemaElement("hashKey", HASH))
                        .withProvisionedThroughput(1L, 1L);
                    for (String org : testArgument.getOrgs()) {
                        List<CreateTableRequest> createTableRequests = TABLE_NAME_PREFIXES.stream()
                            .map(prefix -> baseBuilder.withTableName(prefix + org).build())
                            .collect(Collectors.toList());
                        for (CreateTableRequest createTableRequest : createTableRequests) {
                            mtContext.withContext(org,
                                () -> new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                                    .createTableIfNotExists(createTableRequest, getPollInterval()));
                        }
                    }
                }
            });
        }
    }

    private static class SharedTableArgumentBuilder extends ArgumentBuilder {
        @Override
        public List<TestArgument> get() {
            List<TestArgument> allArgs = super.get();
            return allArgs.stream()
                .filter(a -> a.getAmazonDynamoDb() instanceof MtAmazonDynamoDbBySharedTable)
                .collect(Collectors.toList());
        }
    }
}