package com.salesforce.dynamodbv2.mt.mappers;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtAmazonDynamoDbStreamsByTableTest {

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsByTableTest.class.getSimpleName() + ".";
    private static MtAmazonDynamoDbStreamsBaseTestUtils BASE_TEST_UTILS = new MtAmazonDynamoDbStreamsBaseTestUtils();

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

        MtAmazonDynamoDbByTable mtDynamoDb = MtAmazonDynamoDbByTable.builder()
            .withTablePrefix(tablePrefix)
            .withAmazonDynamoDb(dynamoDb)
            .withContext(MT_CONTEXT)
            .build();
        try {
            TableUtils.createTableIfNotExists(dynamoDb, BASE_TEST_UTILS.newCreateTableRequest(randomTableName, false));

            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            // getting streams before creating tenant tables should return nothing, since we lazily create tables
            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertTrue(streams.isEmpty());

            BASE_TEST_UTILS.createTenantTables(mtDynamoDb);

            // getting streams after creating tenant tables should return as many streams as tenants
            streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertEquals(BASE_TEST_UTILS.TENANTS.length, streams.size());
        } finally {
            BASE_TEST_UTILS.deleteMtTables(mtDynamoDb);
            TableUtils.deleteTableIfExists(dynamoDb, new DeleteTableRequest(randomTableName));
        }
    }

    /**
     * Verifies that GetRecords returns expected MtRecords (both with and without context).
     */
    @Test
    void testRecords() {
        final String tablePrefix = TABLE_PREFIX + "testRecords.";

        final MtAmazonDynamoDbByTable mtDynamoDb = MtAmazonDynamoDbByTable.builder()
            .withTablePrefix(tablePrefix)
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withContext(MT_CONTEXT)
            .build();
        try {
            BASE_TEST_UTILS.createTenantTables(mtDynamoDb);

            int i = 0;
            final MtRecord expected1 = BASE_TEST_UTILS.putTestItem(mtDynamoDb, BASE_TEST_UTILS.TENANTS[0], i++);
            final MtRecord expected2 = BASE_TEST_UTILS.putTestItem(mtDynamoDb, BASE_TEST_UTILS.TENANTS[0], i);
            i = 0;
            final MtRecord expected3 = BASE_TEST_UTILS.putTestItem(mtDynamoDb, BASE_TEST_UTILS.TENANTS[1], i++);
            final MtRecord expected4 = BASE_TEST_UTILS.putTestItem(mtDynamoDb, BASE_TEST_UTILS.TENANTS[1], i);

            // get shard iterators
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            // test without context
            final List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            final List<Record> actual = streams.stream()
                .map(stream -> BASE_TEST_UTILS.getShardIterator(mtDynamoDbStreams, stream))
                .map(Optional::get)
                .map(iterator -> mtDynamoDbStreams.getRecords(new GetRecordsRequest().withShardIterator(iterator))
                    .getRecords())
                .flatMap(List::stream)
                .collect(toList());
            assertRecordsEquals(actual, expected1, expected2, expected3, expected4);

            // test with tenant contexts
            MT_CONTEXT.withContext(BASE_TEST_UTILS.TENANTS[0], () -> {
                String tenantIterator = BASE_TEST_UTILS.getShardIterator(mtDynamoDbStreams, mtDynamoDb).orElseThrow();
                BASE_TEST_UTILS.assertGetRecords(mtDynamoDbStreams, tenantIterator, expected1, expected2);
            });
            MT_CONTEXT.withContext(BASE_TEST_UTILS.TENANTS[1], () -> {
                String tenantIterator = BASE_TEST_UTILS.getShardIterator(mtDynamoDbStreams, mtDynamoDb).orElseThrow();
                BASE_TEST_UTILS.assertGetRecords(mtDynamoDbStreams, tenantIterator, expected3, expected4);
            });
        } finally {
            BASE_TEST_UTILS.deleteMtTables(mtDynamoDb);
        }
    }

    private void assertRecordsEquals(List<Record> actual, MtRecord... expected) {
        assertEquals(expected.length, actual.size());
        for (MtRecord expectedRecord : expected) {
            assertTrue(actual.stream().anyMatch(actualRecord -> BASE_TEST_UTILS.equals(expectedRecord, actualRecord)));
        }
    }

}
