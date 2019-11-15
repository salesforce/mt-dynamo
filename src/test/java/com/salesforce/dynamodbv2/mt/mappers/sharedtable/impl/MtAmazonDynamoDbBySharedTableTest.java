package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TOP_LEVEL_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
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
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbComposite;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.AmazonDynamoDbStrategy;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
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
    void testListTables_noContext(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        final List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();

        assertFalse(allTables.isEmpty());
        MtScanningSnapshotter tableSnapshotter = new MtScanningSnapshotter();
        List<SnapshotResult> snapshots = new ArrayList<>();
        try {
            for (String table : allTables) {
                snapshots.add(tableSnapshotter.snapshotTableToTarget(new SnapshotRequest("fake-backup",
                    table,
                    getSharedTableClient(testArgument.getAmazonDynamoDb()).getBackupTablePrefix() + table,
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
    void testListTables_withContext(TestArgument testArgument) {
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
    void testListTablesPagination_withContext(TestArgument testArgument) {
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
    void testPutItem_ScanColumnsReserved(TestArgument testArgument) {
        MtAmazonDynamoDbBySharedTable mtDynamo = getSharedTableClient(testArgument.getAmazonDynamoDb());
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
    void testUpdateItem_ScanColumnsReserved(TestArgument testArgument) {
        MtAmazonDynamoDbBySharedTable mtDynamo = getSharedTableClient(testArgument.getAmazonDynamoDb());
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

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(SharedTableArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {})
    void testCreateAndDeleteMtTable(TestArgument testArgument) {
        MtAmazonDynamoDb mtDynamo = (MtAmazonDynamoDb) testArgument.getAmazonDynamoDb();
        String tableName = "someMtTable";

        // create table in top-level context
        MT_CONTEXT.withContext(TOP_LEVEL_CONTEXT, () -> {
            CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withKeyType(HASH).withAttributeName(HASH_KEY))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName(HASH_KEY)
                    .withAttributeType(testArgument.getHashKeyAttrType()));
            mtDynamo.createMultitenantTable(request);
        });
        String physicalTableName = testArgument.getAmazonDynamoDbStrategy().name() + "_" + tableName;
        assertTrue(testArgument.getRootAmazonDynamoDb().listTables().getTableNames().contains(physicalTableName));

        // put and get a record in the context of each org
        testArgument.forEachOrgContext(org -> {
            Map<String, AttributeValue> key = ImmutableMap.of(HASH_KEY,
                createAttributeValue(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE));
            mtDynamo.putItem(new PutItemRequest().withTableName(tableName).withItem(key));

            GetItemResult getItemResult = mtDynamo.getItem(new GetItemRequest().withTableName(tableName).withKey(key));
            assertEquals(key, getItemResult.getItem());
        });

        // delete table in top-level context
        MT_CONTEXT.withContext(TOP_LEVEL_CONTEXT, () -> {
            mtDynamo.deleteTable(tableName);
        });
        assertFalse(testArgument.getRootAmazonDynamoDb().listTables().getTableNames().contains(physicalTableName));
    }

    private MtAmazonDynamoDbBySharedTable getSharedTableClient(AmazonDynamoDB dynamoDb) {
        if (dynamoDb instanceof MtAmazonDynamoDbBySharedTable) {
            return (MtAmazonDynamoDbBySharedTable) dynamoDb;
        }
        if (dynamoDb instanceof MtAmazonDynamoDbComposite) {
            AmazonDynamoDB delegate = ((MtAmazonDynamoDbComposite) dynamoDb).getDelegateFromContext();
            return (MtAmazonDynamoDbBySharedTable) delegate;
        }
        throw new IllegalArgumentException("Unable to find MtAmazonDynamoDbBySharedTable: " + dynamoDb);
    }

    private static ArgumentBuilder getSharedTableArgumentBuilder() {
        return new ArgumentBuilder().withStrategies(AmazonDynamoDbStrategy.SHARED_TABLE_STRATEGIES);
    }

    private static class SharedTableArgumentProvider extends DefaultArgumentProvider {
        SharedTableArgumentProvider() {
            super(getSharedTableArgumentBuilder(), new DefaultTestSetup(DefaultTestSetup.ALL_TABLES));
        }
    }

    private static final List<String> TABLE_NAME_PREFIXES = ImmutableList.of("table1-", "table2-", "table3-");

    private static class ListVirtualTableProvider extends DefaultArgumentProvider {
        ListVirtualTableProvider() {
            super(getSharedTableArgumentBuilder(), new DefaultTestSetup(DefaultTestSetup.NO_TABLES) {
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

}