package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.salesforce.dynamodbv2.mt.backups.SnapshotRequest;
import com.salesforce.dynamodbv2.mt.backups.SnapshotResult;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtSharedTableBackupManagerS3It.MtScanningSnapshotter;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultTestSetup;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import java.util.List;
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
        List<SnapshotResult> snapshots = Lists.newArrayList();
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
            List<String> tablesSeen = Lists.newArrayList();
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

    private static class SharedTableArgumentProvider extends DefaultArgumentProvider {
        public SharedTableArgumentProvider() {
            super(new SharedTableArgumentBuilder(), new DefaultTestSetup());
        }
    }

    private static List<String> TABLE_NAME_PREFIXES = ImmutableList.of("table1-", "table2-", "table3-");

    private static class ListVirtualTableProvider extends DefaultArgumentProvider {
        public ListVirtualTableProvider() {
            super(new SharedTableArgumentBuilder(), new DefaultTestSetup() {

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