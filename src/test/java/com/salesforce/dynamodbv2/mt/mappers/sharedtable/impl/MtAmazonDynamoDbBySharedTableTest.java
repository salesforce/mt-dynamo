package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.salesforce.dynamodbv2.mt.backups.SnapshotRequest;
import com.salesforce.dynamodbv2.mt.backups.SnapshotResult;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtSharedTableBackupManagerS3It.MtScanningSnapshotter;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultTestSetup;
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
                        table + MtAmazonDynamoDbBySharedTable.BACKUP_TEMP_TABLE_SUFFIX,
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

    private static class SharedTableArgumentProvider extends DefaultArgumentProvider {
        public SharedTableArgumentProvider() {
            super(new SharedTableArgumentBuilder(), new DefaultTestSetup());
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