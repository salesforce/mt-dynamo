package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBaseTestUtils.assertGetRecords;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningStrategy;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Verifies behavior that applies all multitenant streams implementations.
 */
class MtAmazonDynamoDbStreamsBaseTest {

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsBaseTest.class.getSimpleName() + ".";

    private static class Args implements ArgumentsProvider {

        @Override
        public java.util.stream.Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            String prefix =
                TABLE_PREFIX + context.getTestMethod().orElseThrow(IllegalStateException::new).getName() + ".";

            AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

            MtAmazonDynamoDbBySharedTable indexMtDynamoDb = SharedTableBuilder.builder()
                .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                    .newCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, false))
                .withAmazonDynamoDb(dynamoDb)
                .withTablePrefix(prefix)
                .withContext(MT_CONTEXT)
                .withClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()))
                .build();

            MtAmazonDynamoDbBySharedTable indexBinaryHkMtDynamoDb = SharedTableBuilder.builder()
                .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                    .newCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME, true))
                .withAmazonDynamoDb(dynamoDb)
                .withTablePrefix(prefix)
                .withContext(MT_CONTEXT)
                .withClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()))
                .build();

            MtAmazonDynamoDbBySharedTable indexHashPartitioningMtDynamoDb = SharedTableBuilder.builder()
                .withCreateTableRequests(MtAmazonDynamoDbStreamsBaseTestUtils
                    .newHashPartitioningCreateTableRequest(MtAmazonDynamoDbStreamsBaseTestUtils.SHARED_TABLE_NAME))
                .withAmazonDynamoDb(dynamoDb)
                .withTablePrefix(prefix)
                .withContext(MT_CONTEXT)
                .withPartitioningStrategy(new HashPartitioningStrategy(64))
                .withBinaryHashKey(true)
                .withClock(Clock.fixed(Instant.now(), ZoneId.systemDefault()))
                .build();

            MtAmazonDynamoDbByTable tableMtDynamoDb = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(prefix)
                .withAmazonDynamoDb(dynamoDb)
                .withContext(MT_CONTEXT)
                .build();
            MtAmazonDynamoDbStreams tableMtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(tableMtDynamoDb,
                AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());

            return java.util.stream.Stream.of(
                getArgumentsForSharedTableStrategy(indexMtDynamoDb),
                getArgumentsForSharedTableStrategy(indexBinaryHkMtDynamoDb),
                getArgumentsForSharedTableStrategy(indexHashPartitioningMtDynamoDb),
                Arguments.of(tableMtDynamoDb, tableMtDynamoDbStreams, null, null)
            );
        }

        private Arguments getArgumentsForSharedTableStrategy(MtAmazonDynamoDbBySharedTable indexMtDynamoDb) {
            MeterRegistry indexMtDynamoDbStreamsMeterRegistry = new SimpleMeterRegistry();
            CachingAmazonDynamoDbStreams indexMtDynamoDbStreamsCache =
                new CachingAmazonDynamoDbStreams.Builder(AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal())
                    .withMeterRegistry(indexMtDynamoDbStreamsMeterRegistry)
                    .build();
            MtAmazonDynamoDbStreams indexMtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(indexMtDynamoDb,
                indexMtDynamoDbStreamsCache);
            return Arguments.of(indexMtDynamoDb, indexMtDynamoDbStreams, indexMtDynamoDbStreamsCache,
                indexMtDynamoDbStreamsMeterRegistry);
        }
    }

    // work-around for command-line build: some previous tests don't seem to be clearing the mt context
    @BeforeEach
    void before() {
        MT_CONTEXT.setContext(null);
    }

    /**
     * Verifies that the streams API can be used in a consistent way over the different mt strategies (KCL style).
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testStream(MtAmazonDynamoDbBase mtDynamoDb, MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        try {
            // create tenant tables and test data
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);
            final Collection<MtRecord> expected = new ArrayList<>(4);
            int i = 0;
            expected.add(MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i++));
            expected.add(MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i));
            i = 0;
            expected.add(MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++));
            expected.add(MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i));

            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();

            // we are not asserting how many streams we get back, since that's strategy-specific
            List<String> iterators = streams.stream()
                .map(stream -> MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, stream))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
            // we are also not asserting which stream returns which records, just that we get them all
            assertGetRecords(mtDynamoDbStreams, iterators, expected);
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Verifies that tenant views of tables work as expected.
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testTableStream(MtAmazonDynamoDbBase mtDynamoDb, MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        try {
            // create tenant tables and test data
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);
            int i = 0;
            final MtRecord expected1 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i++);
            final MtRecord expected2 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], i);
            i = 0;
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i++);
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], i);

            MT_CONTEXT.withContext(MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], () -> {
                String streamArn = mtDynamoDb.describeTable(MtAmazonDynamoDbStreamsBaseTestUtils.TENANT_TABLE_NAME)
                    .getTable()
                    .getLatestStreamArn();
                assertNotNull(streamArn);
                String iterator = MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, streamArn)
                    .orElseThrow();
                final int expectedRecordCount = mtDynamoDbStreams instanceof MtAmazonDynamoDbStreamsByTable ? 2 : 4;
                assertGetRecords(mtDynamoDbStreams, iterator, expectedRecordCount, expected1, expected2);
            });
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Verifies that reading virtual tables with streams disabled doesn't return records.
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testDisabledStreams(MtAmazonDynamoDbBase mtDynamoDb, MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        try {
            String tenant = MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0];

            // create table with streams enabled
            String tableWithStreamsEnabled = MtAmazonDynamoDbStreamsBaseTestUtils.TENANT_TABLE_NAME
                + "_streams_enabled";
            CreateTableRequest createTableRequestStreamsEnabled =
                MtAmazonDynamoDbStreamsBaseTestUtils.newCreateTableRequest(tableWithStreamsEnabled, false)
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamEnabled(true)
                        .withStreamViewType(NEW_AND_OLD_IMAGES));
            MT_CONTEXT.withContext(tenant, () -> mtDynamoDb.createTable(createTableRequestStreamsEnabled));

            // create table with streams disabled
            String tableWithStreamsDisabled = MtAmazonDynamoDbStreamsBaseTestUtils.TENANT_TABLE_NAME
                + "_streams_disabled";
            CreateTableRequest createTableRequestStreamsDisabled =
                MtAmazonDynamoDbStreamsBaseTestUtils.newCreateTableRequest(tableWithStreamsDisabled, false)
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamEnabled(false)
                    );
            MT_CONTEXT.withContext(tenant, () -> mtDynamoDb.createTable(createTableRequestStreamsDisabled));

            // put an item in each table
            final MtRecord streamsEnabledRecord = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, tableWithStreamsEnabled, tenant, 1);
            MtAmazonDynamoDbStreamsBaseTestUtils.putTestItem(mtDynamoDb, tableWithStreamsDisabled, tenant, 2);

            // verify that the record from the stream enabled table is returned
            MT_CONTEXT.withContext(tenant, () -> {
                String streamArn = mtDynamoDb.describeTable(tableWithStreamsEnabled).getTable().getLatestStreamArn();
                assertNotNull(streamArn);
                String iterator = MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, streamArn)
                    .orElseThrow();
                final int expectedRecordCount = mtDynamoDbStreams instanceof MtAmazonDynamoDbStreamsByTable ? 1 : 2;
                assertGetRecords(mtDynamoDbStreams, iterator, expectedRecordCount, streamsEnabledRecord);
            });

            // verify that either the streamArn is null(for ByTable or ByAccount) or the iterator returns no records
            MT_CONTEXT.withContext(tenant, () -> {
                String streamArn = mtDynamoDb.describeTable(tableWithStreamsDisabled).getTable().getLatestStreamArn();
                if (streamArn != null) {
                    // for byIndex, the streamArn will not be null, but it should return no records
                    String iterator = MtAmazonDynamoDbStreamsBaseTestUtils
                        .getShardIterator(mtDynamoDbStreams, streamArn).orElseThrow();
                    assertGetRecords(mtDynamoDbStreams, iterator, 1);
                }
            });
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Verifies that clients find records with 'after' iterator even if there are gaps, i.e., records inserted by other
     * tenants.
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testGap(MtAmazonDynamoDbBase mtDynamoDb, MtAmazonDynamoDbStreams mtDynamoDbStreams) {
        try {
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);
            final MtRecord expected1 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], 0);
            MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[1], 1);
            final MtRecord expected2 = MtAmazonDynamoDbStreamsBaseTestUtils
                .putTestItem(mtDynamoDb, MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], 3);

            MT_CONTEXT.withContext(MtAmazonDynamoDbStreamsBaseTestUtils.TENANTS[0], () -> {
                String streamArn = mtDynamoDb.describeTable(MtAmazonDynamoDbStreamsBaseTestUtils.TENANT_TABLE_NAME)
                    .getTable()
                    .getLatestStreamArn();

                // first start at trim horizon
                String thIterator = MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, streamArn)
                    .orElseThrow();
                final int expectedRecordCount = mtDynamoDbStreams instanceof MtAmazonDynamoDbStreamsByTable ? 1 : 2;
                String lastSn = assertGetRecords(mtDynamoDbStreams, thIterator, 1, expectedRecordCount, expected1)
                    .getLastSequenceNumber();
                assertNotNull(lastSn);

                // now start at last record: expect to get next record
                String afterIterator = MtAmazonDynamoDbStreamsBaseTestUtils
                    .getShardIterator(mtDynamoDbStreams, streamArn, AFTER_SEQUENCE_NUMBER, lastSn)
                    .orElseThrow();
                assertGetRecords(mtDynamoDbStreams, afterIterator, 1, expected2);
            });
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Tests that the describeStreamCache is utilized when describeStream is called in StreamsBase.
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testDescribeStreamCache(MtAmazonDynamoDbBase mtDynamoDb, MtAmazonDynamoDbStreams mtDynamoDbStreams,
                                 CachingAmazonDynamoDbStreams cachingStreams, MeterRegistry meterRegistry) {

        // Some of the parameterized inputs don't use a CachingAmazonDynamoDbStreams instance
        if (cachingStreams == null) {
            return;
        }

        try {
            MtAmazonDynamoDbStreamsBaseTestUtils.createTenantTables(mtDynamoDb);
            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            String prefix = CachingAmazonDynamoDbStreams.class.getSimpleName();
            long expectedHits = 0L;
            long expectedMisses = 1L;

            for (Stream stream : streams) {
                assertEquals(expectedHits, meterRegistry.get("cache.gets").tags("cache", prefix + ".DescribeStream")
                    .tags("result", "hit").functionCounter().count());

                // Trigger flow that interacts with the describe stream cache (miss)
                MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, stream);

                // Trigger cache hit
                MtAmazonDynamoDbStreamsBaseTestUtils.getShardIterator(mtDynamoDbStreams, stream);
                expectedHits++;

                assertEquals(expectedMisses, meterRegistry.get("cache.gets").tags("cache", prefix + ".DescribeStream")
                    .tags("result", "miss").functionCounter().count());

                assertEquals(expectedHits, meterRegistry.get("cache.gets").tags("cache", prefix + ".DescribeStream")
                    .tags("result", "hit").functionCounter().count());
            }
        } finally {
            MtAmazonDynamoDbStreamsBaseTestUtils.deleteMtTables(mtDynamoDb);
        }
    }
}