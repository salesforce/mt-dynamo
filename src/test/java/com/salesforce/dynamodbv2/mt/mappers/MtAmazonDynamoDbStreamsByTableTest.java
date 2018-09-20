package com.salesforce.dynamodbv2.mt.mappers;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class MtAmazonDynamoDbStreamsByTableTest extends MtAmazonDynamoDbStreamsBaseTest {

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsByTableTest.class.getSimpleName() + ".";

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
            TableUtils.createTableIfNotExists(dynamoDb, newCreateTableRequest(randomTableName));

            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            // getting streams before creating tenant tables should return nothing, since we lazily create tables
            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertTrue(streams.isEmpty());

            createTenantTables(mtDynamoDb);

            // getting streams after creating tenant tables should return as many streams as tenants
            streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertEquals(TENANTS.length, streams.size());
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
        final String tablePrefix = TABLE_PREFIX + "testRecords.";

        final MtAmazonDynamoDbByTable mtDynamoDb = MtAmazonDynamoDbByTable.builder()
            .withTablePrefix(tablePrefix)
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withContext(MT_CONTEXT)
            .build();
        try {
            createTenantTables(mtDynamoDb);

            int i = 0;
            final MtRecord expected1 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            final MtRecord expected2 = putTestItem(mtDynamoDb, TENANTS[0], i);
            i = 0;
            final MtRecord expected3 = putTestItem(mtDynamoDb, TENANTS[1], i++);
            final MtRecord expected4 = putTestItem(mtDynamoDb, TENANTS[1], i);

            // get shard iterators
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            final List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            final Function<String, Stream> getTenantStream = tenant -> streams.stream()
                .filter(matchesTenant(mtDynamoDb, tenant))
                .findFirst()
                .orElseThrow(AssertionFailedError::new);

            final Stream tenant1Stream = getTenantStream.apply(TENANTS[0]);
            final String iterator1 = getShardIterator(mtDynamoDbStreams, tenant1Stream).get();
            assertGetRecords(mtDynamoDbStreams, iterator1, expected1, expected2);
            MT_CONTEXT.withContext(TENANTS[0],
                () -> assertGetRecords(mtDynamoDbStreams, iterator1, expected1, expected2));
            // should we fail here instead of returning no records?
            MT_CONTEXT.withContext(TENANTS[1],
                () -> assertGetRecords(mtDynamoDbStreams, iterator1));

            final Stream tenant2Stream = getTenantStream.apply(TENANTS[1]);
            final String iterator2 = getShardIterator(mtDynamoDbStreams, tenant2Stream).get();
            assertGetRecords(mtDynamoDbStreams, iterator2, expected3, expected4);
            MT_CONTEXT.withContext(TENANTS[0],
                () -> assertGetRecords(mtDynamoDbStreams, iterator2));
            // should we fail here instead of returning no records?
            MT_CONTEXT.withContext(TENANTS[1],
                () -> assertGetRecords(mtDynamoDbStreams, iterator2, expected3, expected4));
        } finally {
            deleteMtTables(mtDynamoDb);
        }
    }

    private static Predicate<Stream> matchesTenant(MtAmazonDynamoDbByTable mtDynamoDb, String tenant) {
        return stream -> tenant.equals(mtDynamoDb.getTenantAndTableName(stream.getTableName())[0]);
    }

}
