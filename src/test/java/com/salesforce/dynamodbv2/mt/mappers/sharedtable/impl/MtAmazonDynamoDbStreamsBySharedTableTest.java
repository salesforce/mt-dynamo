package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.CachingAmazonDynamoDbStreamsTest.CountingAmazonDynamoDbStreams;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests shared table streams.
 */
class MtAmazonDynamoDbStreamsBySharedTableTest {

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsBySharedTableTest.class.getSimpleName() + ".";
    private static final String SHARED_TABLE_NAME = "SharedTable";
    private static final String TENANT_TABLE_NAME = "TenantTable";
    private static final String[] TENANTS = {"tenant1", "tenant2"};

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    /**
     * Test utility method.
     */
    private static CreateTableRequest newCreateTableRequest(String tableName) {
        return new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
            .withAttributeDefinitions(
                new AttributeDefinition(ID_ATTR_NAME, S),
                new AttributeDefinition(INDEX_ID_ATTR_NAME, S))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                .withIndexName("index")
                .withKeySchema(new KeySchemaElement(INDEX_ID_ATTR_NAME, HASH))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            )
            .withStreamSpecification(new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(NEW_AND_OLD_IMAGES));
    }

    private static void deleteSharedTables(MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        mtDynamoDb.getSharedTables().stream()
            .map(CreateTableRequest::getTableName)
            .forEach(name -> TableUtils.deleteTableIfExists(mtDynamoDb.getAmazonDynamoDb(),
                new DeleteTableRequest(name)));
    }

    private static String getShardIterator(MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        ListStreamsResult lsResult = mtDynamoDbStreams.listStreams(new ListStreamsRequest());
        assertEquals(1, lsResult.getStreams().size());
        String streamArn = lsResult.getStreams().get(0).getStreamArn();

        DescribeStreamResult dsResult =
            mtDynamoDbStreams.describeStream(new DescribeStreamRequest().withStreamArn(streamArn));
        assertEquals(1, dsResult.getStreamDescription().getShards().size());
        String shardId = dsResult.getStreamDescription().getShards().get(0).getShardId();

        return mtDynamoDbStreams.getShardIterator(new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)).getShardIterator();
    }

    private static void createTenantTables(AmazonDynamoDB mtDynamoDb) {
        for (String tenant : TENANTS) {
            MT_CONTEXT.withContext(tenant, () -> mtDynamoDb.createTable(newCreateTableRequest(TENANT_TABLE_NAME)));
        }
    }

    /**
     * Puts an item into the table in the given tenant context for the given id and returns the expected MT record.
     */
    private static MtRecord putTestItem(AmazonDynamoDB dynamoDb, String tenant, int id) {
        return MT_CONTEXT.withContext(tenant, sid -> {
            PutItemRequest put = new PutItemRequest(TENANT_TABLE_NAME, ImmutableMap.of(
                ID_ATTR_NAME, new AttributeValue(sid),
                INDEX_ID_ATTR_NAME, new AttributeValue(String.valueOf(id * 10))));
            dynamoDb.putItem(put);
            return new MtRecord()
                .withContext(tenant)
                .withTableName(put.getTableName())
                .withDynamodb(new StreamRecord()
                    .withKeys(ImmutableMap.of(ID_ATTR_NAME, put.getItem().get(ID_ATTR_NAME)))
                    .withNewImage(put.getItem()));
        }, String.valueOf(id));
    }

    private static void assertMtRecord(MtRecord expected, Record actual) {
        assertTrue(actual instanceof MtRecord);
        MtRecord mtRecord = (MtRecord) actual;
        assertEquals(expected.getContext(), mtRecord.getContext());
        assertEquals(expected.getTableName(), mtRecord.getTableName());
        assertEquals(expected.getDynamodb().getKeys(), actual.getDynamodb().getKeys());
        assertEquals(expected.getDynamodb().getNewImage(), actual.getDynamodb().getNewImage());
        assertEquals(expected.getDynamodb().getOldImage(), actual.getDynamodb().getOldImage());
    }

    private static void assertGetRecords(MtAmazonDynamoDbStreams streams, String iterator, MtRecord... expected) {
        GetRecordsResult result = streams.getRecords(new GetRecordsRequest().withShardIterator(iterator));
        assertNotNull(result.getNextShardIterator());
        List<Record> records = result.getRecords();
        assertEquals(expected.length, records.size());
        for (int i = 0; i < expected.length; i++) {
            assertMtRecord(expected[i], records.get(i));
        }
    }

    // work-around for command-line build: some previous tests don't seem to be clearing the mt context
    @BeforeEach
    void before() {
        MT_CONTEXT.setContext(null);
    }

    /**
     * Verifies that list streams returns only streams for mt tables for the given client.
     */
    @Test
    void testListStreams() {
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        String tablePrefix = TABLE_PREFIX + "testListStreams.";
        String randomTableName = "RandomTable";

        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
            .withCreateTableRequests(newCreateTableRequest(SHARED_TABLE_NAME))
            .withAmazonDynamoDb(dynamoDb)
            .withTablePrefix(tablePrefix)
            .withPrecreateTables(true)
            .withContext(() -> Optional.empty())
            .build();
        try {
            TableUtils.createTableIfNotExists(dynamoDb, newCreateTableRequest(randomTableName));

            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertEquals(1, streams.size());

            Stream stream = streams.get(0);
            assertEquals(tablePrefix + SHARED_TABLE_NAME, stream.getTableName());
            assertNotNull(stream.getStreamArn());
            assertNotNull(stream.getStreamLabel());
        } finally {
            deleteSharedTables(mtDynamoDb);
            TableUtils.deleteTableIfExists(dynamoDb, new DeleteTableRequest(randomTableName));
        }
    }


    /**
     * Verifies that GetRecords returns expected MtRecords (both with and without context).
     */
    @Test
    void testRecords() {
        String tablePrefix = TABLE_PREFIX + "testRecords.";

        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
            .withCreateTableRequests(newCreateTableRequest(SHARED_TABLE_NAME))
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withTablePrefix(tablePrefix)
            .withPrecreateTables(true)
            .withContext(MT_CONTEXT)
            .build();
        try {
            createTenantTables(mtDynamoDb);

            int i = 0;
            MtRecord expected1 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            MtRecord expected2 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            i = 0;
            MtRecord expected3 = putTestItem(mtDynamoDb, TENANTS[1], i++);
            MtRecord expected4 = putTestItem(mtDynamoDb, TENANTS[1], i++);

            // get shard iterator
            CountingAmazonDynamoDbStreams dynamoDbStreams =
                new CountingAmazonDynamoDbStreams(AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                dynamoDbStreams);
            String iterator = getShardIterator(mtDynamoDbStreams);

            // test without context
            assertGetRecords(mtDynamoDbStreams, iterator, expected1, expected2, expected3, expected4);
            // test with each tenant context
            MT_CONTEXT.withContext(TENANTS[0],
                () -> assertGetRecords(mtDynamoDbStreams, iterator, expected1, expected2));
            MT_CONTEXT.withContext(TENANTS[1],
                () -> assertGetRecords(mtDynamoDbStreams, iterator, expected3, expected4));

            // once to fetch records, once per getRecords (3) to find empty range
            assertEquals(4, dynamoDbStreams.getRecordsCount);
            assertEquals(1, dynamoDbStreams.getShardIteratorCount);
        } finally {
            deleteSharedTables(mtDynamoDb);
        }
    }

    /**
     * Verifies that GetRecords attempts to fetch multiple pages if result below limit after filtering, but stops if
     * next page contains more records than needed.
     */
    @Test
    void testLimit() {
        String tablePrefix = TABLE_PREFIX + "testLimit.";

        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
            .withCreateTableRequests(newCreateTableRequest(SHARED_TABLE_NAME))
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withTablePrefix(tablePrefix)
            .withPrecreateTables(true)
            .withContext(MT_CONTEXT)
            .build();
        try {
            // create tenant tables
            createTenantTables(mtDynamoDb);

            int i = 0;
            // one record for tenant 1 on page 1 (expect to get that record)
            putTestItem(mtDynamoDb, TENANTS[1], i++);
            final MtRecord expected1 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            putTestItem(mtDynamoDb, TENANTS[1], i++);
            // one record for tenant 1 on page 2 (expect to get that record)
            putTestItem(mtDynamoDb, TENANTS[1], i++);
            final MtRecord expected2 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            putTestItem(mtDynamoDb, TENANTS[1], i++);
            // two records for tenant 1 on page 3 (expect to get neither)
            putTestItem(mtDynamoDb, TENANTS[0], i++);
            putTestItem(mtDynamoDb, TENANTS[0], i++);
            putTestItem(mtDynamoDb, TENANTS[1], i++);

            // now query change streams
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());
            String iterator = getShardIterator(mtDynamoDbStreams);

            MT_CONTEXT.withContext(TENANTS[0], () -> {
                GetRecordsResult result = mtDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(iterator)
                    .withLimit(3));
                assertNotNull(result.getNextShardIterator());

                // we only expect to see 2 records, since the last page would exceed limit
                assertEquals(2, result.getRecords().size());
                Iterator<Record> it = result.getRecords().iterator();
                assertMtRecord(expected1, it.next());
                assertMtRecord(expected2, it.next());
            });
        } finally {
            deleteSharedTables(mtDynamoDb);
        }
    }
}
