package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.BillingMode.PAY_PER_REQUEST;
import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBaseTestUtils;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.util.MockTicker;
import com.salesforce.dynamodbv2.testsupport.CountingAmazonDynamoDbStreams;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
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

    // helper method that assumes there is only one shared table stream
    private static String getShardIterator(MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        ListStreamsResult lsResult = mtDynamoDbStreams.listStreams(new ListStreamsRequest());
        List<String> iterators = lsResult.getStreams().stream()
            .map(stream -> MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, stream))
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
            .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                .newCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, false))
            .withAmazonDynamoDb(dynamoDb)
            .withTablePrefix(tablePrefix)
            .withCreateTablesEagerly(true)
            .withContext(Optional::empty)
            .withClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()))
            .build();
        try {
            TableUtils.createTableIfNotExists(dynamoDb,
                MtAmazonDynamoDbStreamsBaseTestUtils.newCreateTableRequest(randomTableName, false));

            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertEquals(1, streams.size());

            Stream stream = streams.get(0);
            assertEquals(tablePrefix + MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, stream.getTableName());
            assertNotNull(stream.getStreamArn());
            assertNotNull(stream.getStreamLabel());
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
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
            .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                .newCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, true))
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withTablePrefix(tablePrefix)
            .withCreateTablesEagerly(true)
            .withContext(MT_CONTEXT)
            .build();
        try {
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);

            int i = 0;
            MtRecord expected1 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i++);
            MtRecord expected2 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i);
            i = 0;
            MtRecord expected3 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++);
            MtRecord expected4 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i);

            // get shard iterator
            CountingAmazonDynamoDbStreams dynamoDbStreams =
                new CountingAmazonDynamoDbStreams(AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                new CachingAmazonDynamoDbStreams.Builder(dynamoDbStreams).withTicker(new MockTicker()).build());

            // test without context
            String iterator = getShardIterator(mtDynamoDbStreams);
            MtAmazonDynamoDbStreamsBaseTestUtils
                .assertGetRecords(mtDynamoDbStreams, iterator, expected1, expected2, expected3, expected4);

            // test with each tenant context
            MT_CONTEXT.withContext(MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], () -> {
                String tenantIterator = MtAmazonDynamoDbStreamsBaseTestUtils
                    .getShardIterator(mtDynamoDbStreams, mtDynamoDb).orElseThrow();
                MtAmazonDynamoDbStreamsBaseTestUtils
                    .assertGetRecords(mtDynamoDbStreams, tenantIterator, expected1, expected2);
            });
            MT_CONTEXT.withContext(MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], () -> {
                String tenantIterator = MtAmazonDynamoDbStreamsBaseTestUtils
                    .getShardIterator(mtDynamoDbStreams, mtDynamoDb).orElseThrow();
                MtAmazonDynamoDbStreamsBaseTestUtils
                    .assertGetRecords(mtDynamoDbStreams, tenantIterator, expected3, expected4);
            });

            // once per fetch (since they are all trim horizon)
            assertEquals(2, dynamoDbStreams.getRecordsCount);
            assertEquals(1, dynamoDbStreams.getShardIteratorCount);
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
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
            .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                .newCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, true))
            .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
            .withTablePrefix(tablePrefix)
            .withCreateTablesEagerly(true)
            .withContext(MT_CONTEXT)
            .build();
        try {
            // create tenant tables
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);

            int i = 0;
            // one record for tenant 1 on page 1 (expect to get that record)
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++);
            final MtRecord expected1 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i++);
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++);
            // one record for tenant 1 on page 2 (also expect to get that one)
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++);
            final MtRecord expected2 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i++);
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i);

            // now query change streams
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            MT_CONTEXT.withContext(MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], () -> {
                String iterator = MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, mtDynamoDb)
                    .orElseThrow();
                GetRecordsResult result = mtDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(iterator)
                    .withLimit(3));
                assertNotNull(result.getNextShardIterator());

                // we only expect to see 2 records, since the last page would exceed limit
                assertEquals(2, result.getRecords().size());
                Iterator<Record> it = result.getRecords().iterator();
                MtAmazonDynamoDbStreamsBaseTestUtils.assertMtRecord(expected1, it.next());
                MtAmazonDynamoDbStreamsBaseTestUtils.assertMtRecord(expected2, it.next());
            });
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Verify that timeout stops iterating over shard even if more records are available.
     */
    @Test
    void testTimeout() {
        /* ARRANGE (lots of stuff) */

        final String tablePrefix = TABLE_PREFIX + "testTimeout.";

        // second clock tick is higher than limit
        final Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(1L).thenReturn(3L);

        final String mockArn = "arn:aws:dynamodb:region:account-id:table/tableName/stream/label";
        final String mockMtArn = mockArn + "/context/T1/tenantTable/tenantTableName";

        // two get records calls that return max records (we expect only first call to happen due to timeout)
        // every 10th record is for tenant (so we would get more records if it weren't for timeout)
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        when(streams.getShardIterator(any())).thenReturn(
            new GetShardIteratorResult().withShardIterator(mockArn + "|it"));
        when(streams.getRecords(any()))
            .thenReturn(new GetRecordsResult().withNextShardIterator(mockArn + "|it2")
                .withRecords(mockRecords(0, 1000)))
            .thenReturn(new GetRecordsResult().withNextShardIterator(mockArn + "|it3")
                .withRecords(mockRecords(1000, 1000)));

        final MtAmazonDynamoDbBySharedTable mtDynamo = createMtAmazonDynamoDb(tablePrefix, clock);

        // finally create SUT
        final MtAmazonDynamoDbStreamsBySharedTable sharedTableStreams =
            new MtAmazonDynamoDbStreamsBySharedTable(streams, mtDynamo);

        /* ACT */
        GetRecordsResult result = MT_CONTEXT.withContext("T1", i -> {
            mtDynamo.createTable(new CreateTableRequest()
                .withTableName("tenantTableName")
                .withKeySchema(
                    new KeySchemaElement("vhk", HASH))
                .withAttributeDefinitions(
                    new AttributeDefinition("vhk", S)
                )
                .withBillingMode(PAY_PER_REQUEST)
            );
            GetShardIteratorResult iteratorResult = sharedTableStreams.getShardIterator(
                new GetShardIteratorRequest().withStreamArn(mockMtArn).withShardId("shard")
                    .withShardIteratorType(AFTER_SEQUENCE_NUMBER).withSequenceNumber("1"));
            return sharedTableStreams.getRecords(
                new GetRecordsRequest().withShardIterator(iteratorResult.getShardIterator()));
        }, null);

        /* ASSERT */

        // expect only 100 records return (as opposed to two hundred if it weren't for timeout)
        assertEquals(100, result.getRecords().size());
        assertEquals(mockMtArn + "|it2", result.getNextShardIterator());
    }

    /**
     * Verifies that second page of records is retried if the tenant records in it exceed the limit.
     */
    @Test
    void testRetry() {
        /* ARRANGE (lots of stuff) */

        final String tablePrefix = TABLE_PREFIX + "testRetry.";

        // fix clock (so that we don't run out of time)
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        final String mockArn = "arn:aws:dynamodb:region:account-id:table/tableName/stream/label";
        final String mockMtArn = mockArn + "/context/T1/tenantTable/tenantTableName";

        // two get records calls that return max records (we expect only first call to happen due to timeout)
        // every 10th record is for tenant (so we would get more records if it weren't for timeout)
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        when(streams.getShardIterator(any())).thenReturn(
            new GetShardIteratorResult().withShardIterator(mockArn + "|it0"));
        when(streams.getRecords(eq(new GetRecordsRequest().withLimit(1000).withShardIterator(mockArn + "|it0"))))
            .thenReturn(new GetRecordsResult().withNextShardIterator(mockArn + "|it1000")
                .withRecords(mockRecords(0, 1000)));
        when(streams.getRecords(eq(new GetRecordsRequest().withLimit(1000).withShardIterator(mockArn + "|it1000"))))
            .thenReturn(new GetRecordsResult().withNextShardIterator(mockArn + "|it2000")
                .withRecords(mockRecords(1000, 1000)));
        when(streams.getRecords(eq(new GetRecordsRequest().withLimit(500).withShardIterator(mockArn + "|it1000"))))
            .thenReturn(new GetRecordsResult().withNextShardIterator(mockArn + "|it1500")
                .withRecords(mockRecords(1000, 500)));

        final MtAmazonDynamoDbBySharedTable mtDynamo = createMtAmazonDynamoDb(tablePrefix, clock);

        // finally create SUT
        final MtAmazonDynamoDbStreamsBySharedTable sharedTableStreams =
            new MtAmazonDynamoDbStreamsBySharedTable(streams, mtDynamo);

        /* ACT */
        GetRecordsResult result = MT_CONTEXT.withContext("T1", i -> {
            mtDynamo.createTable(new CreateTableRequest()
                .withTableName("tenantTableName")
                .withKeySchema(
                    new KeySchemaElement("vhk", HASH))
                .withAttributeDefinitions(
                    new AttributeDefinition("vhk", S)
                )
                .withBillingMode(PAY_PER_REQUEST)
            );
            GetShardIteratorResult iteratorResult = sharedTableStreams.getShardIterator(
                new GetShardIteratorRequest().withStreamArn(mockMtArn).withShardId("shard")
                    .withShardIteratorType(AFTER_SEQUENCE_NUMBER).withSequenceNumber("1"));
            return sharedTableStreams.getRecords(
                new GetRecordsRequest().withLimit(150).withShardIterator(iteratorResult.getShardIterator()));
        }, null);

        /* ASSERT */

        // expect only 100 records return (as opposed to two hundred if it weren't for timeout)
        assertEquals(150, result.getRecords().size());
        assertEquals(mockMtArn + "|it1500", result.getNextShardIterator());
    }

    /**
     * Verifies that getRecords for tenant does not retry for empty results.
     */
    @Test
    void testRetryNoRecords() {
        /* ARRANGE (lots of stuff) */

        final String tablePrefix = TABLE_PREFIX + "testRetryNoRecords.";

        // fix clock (so that we don't run out of time)
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        final String mockArn = "arn:aws:dynamodb:region:account-id:table/tableName/stream/label";
        final String mockMtArn = mockArn + "/context/T1/tenantTable/tenantTableName";
        final String iterator = mockArn + "|iterator";
        final String nextIterator = mockArn + "|nextIterator";

        final MtAmazonDynamoDbBySharedTable mtDynamo = createMtAmazonDynamoDb(tablePrefix, clock);
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        when(streams.getShardIterator(any())).thenReturn(new GetShardIteratorResult().withShardIterator(iterator));
        when(streams.getRecords(new GetRecordsRequest().withShardIterator(iterator).withLimit(1000)))
            .thenReturn(new GetRecordsResult().withRecords().withNextShardIterator(nextIterator));
        when(streams.getRecords(new GetRecordsRequest().withShardIterator(nextIterator).withLimit(1000)))
            .thenThrow(new AssertionError());
        final MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamo, streams);

        /* ACT */
        GetRecordsResult result = MT_CONTEXT.withContext("T1", i -> {
            mtDynamo.createTable(new CreateTableRequest()
                .withTableName("tenantTableName")
                .withKeySchema(
                    new KeySchemaElement("vhk", HASH))
                .withAttributeDefinitions(
                    new AttributeDefinition("vhk", S)
                )
                .withStreamSpecification(new StreamSpecification()
                    .withStreamEnabled(true)
                    .withStreamViewType(NEW_AND_OLD_IMAGES))
                .withBillingMode(PAY_PER_REQUEST)
            );
            final String shardIterator = mtDynamoDbStreams.getShardIterator(
                new GetShardIteratorRequest().withStreamArn(mockMtArn).withShardId("shard")
                    .withShardIteratorType(AFTER_SEQUENCE_NUMBER).withSequenceNumber("1")).getShardIterator();
            return mtDynamoDbStreams.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));
        }, null);

        /* ASSERT */

        // expect no records (and no retry attempt)
        assertEquals(0, result.getRecords().size());
    }

    private static MtAmazonDynamoDbBySharedTable createMtAmazonDynamoDb(String prefix, Clock clock) {
        final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        return SharedTableBuilder.builder()
            .withAmazonDynamoDb(amazonDynamoDB)
            .withClock(clock)
            .withContext(MT_CONTEXT)
            .withGetRecordsTimeLimit(1L)
            .withTablePrefix(prefix)
            .withCreateTableRequests(new CreateTableRequest()
                .withTableName("TestTable")
                .withKeySchema(
                    new KeySchemaElement("hk", HASH))
                .withAttributeDefinitions(
                    new AttributeDefinition("hk", S)
                )
                .withStreamSpecification(new StreamSpecification()
                    .withStreamEnabled(true)
                    .withStreamViewType(NEW_AND_OLD_IMAGES))
                .withBillingMode(PAY_PER_REQUEST))
            .build();
    }

    private static List<Record> mockRecords(int start, int num) {
        List<Record> records = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            final String val = (i % 10 == 0 ? "T1" : "T2") + "/tenantTableName/" + i;
            records.add(new Record().withDynamodb(new StreamRecord()
                .withKeys(ImmutableMap.of("hk", new AttributeValue(val)))
                .withSequenceNumber(String.valueOf(start + i))));
        }
        return records;
    }
}
