package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBaseTest;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.testsupport.CountingAmazonDynamoDbStreams;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests shared table streams.
 */
class MtAmazonDynamoDbStreamsBySharedTableTest extends MtAmazonDynamoDbStreamsBaseTest {

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsBySharedTableTest.class.getSimpleName() + ".";

    // helper method that assumes there is only one shared table stream
    private static String getShardIterator(MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        ListStreamsResult lsResult = mtDynamoDbStreams.listStreams(new ListStreamsRequest());
        List<String> iterators = lsResult.getStreams().stream()
            .map(stream -> getShardIterator(mtDynamoDbStreams, stream))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toList());
        assertEquals(1, iterators.size());
        return iterators.get(0);
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
            .withContext(Optional::empty)
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
            deleteMtTables(mtDynamoDb);
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
            MtRecord expected2 = putTestItem(mtDynamoDb, TENANTS[0], i);
            i = 0;
            MtRecord expected3 = putTestItem(mtDynamoDb, TENANTS[1], i++);
            MtRecord expected4 = putTestItem(mtDynamoDb, TENANTS[1], i);

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
            deleteMtTables(mtDynamoDb);
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
            putTestItem(mtDynamoDb, TENANTS[1], i);

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
            deleteMtTables(mtDynamoDb);
        }
    }

}
