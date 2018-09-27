package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
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
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Verifies behavior that applies all multitenant streams implementations.
 */
public class MtAmazonDynamoDbStreamsBaseTest {

    protected static final String SHARED_TABLE_NAME = "SharedTable";
    protected static final String[] TENANTS = {"tenant1", "tenant2"};

    private static final String TABLE_PREFIX = MtAmazonDynamoDbStreamsTest.class.getSimpleName() + ".";
    private static final String TENANT_TABLE_NAME = "TenantTable";
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    /**
     * Test utility method.
     */
    protected static CreateTableRequest newCreateTableRequest(String tableName) {
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

    protected static void createTenantTables(AmazonDynamoDB mtDynamoDb) {
        for (String tenant : TENANTS) {
            MT_CONTEXT.withContext(tenant, () -> mtDynamoDb.createTable(newCreateTableRequest(TENANT_TABLE_NAME)));
        }
    }

    protected static void deleteMtTables(MtAmazonDynamoDbBase mtDynamoDb) {
        mtDynamoDb.listTables().getTableNames().stream().filter(mtDynamoDb::isMtTable).forEach(
            name -> TableUtils.deleteTableIfExists(mtDynamoDb.getAmazonDynamoDb(),
                new DeleteTableRequest(name)));
    }

    protected static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams, Stream stream) {
        return getShardIterator(mtDynamoDbStreams, stream.getStreamArn());
    }

    private static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams, String streamArn) {
        DescribeStreamResult dsResult = mtDynamoDbStreams.describeStream(
            new DescribeStreamRequest().withStreamArn(streamArn));

        if (StreamStatus.fromValue(dsResult.getStreamDescription().getStreamStatus()) != StreamStatus.ENABLED) {
            return Optional.empty();
        }

        // assumes we are running against local MT dynamo that has one shard per stream
        Assumptions.assumeTrue(dsResult.getStreamDescription().getShards().size() == 1);
        String shardId = dsResult.getStreamDescription().getShards().get(0).getShardId();

        return Optional.of(mtDynamoDbStreams.getShardIterator(new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)).getShardIterator());
    }

    /**
     * Puts an item into the table in the given tenant context for the given {@code id} and returns the expected MT
     * record.
     */
    protected static MtRecord putTestItem(AmazonDynamoDB dynamoDb, String tenant, int id) {
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

    protected static void assertMtRecord(MtRecord expected, Record actual) {
        assertTrue(actual instanceof MtRecord);
        MtRecord mtRecord = (MtRecord) actual;
        assertEquals(expected.getContext(), mtRecord.getContext());
        assertEquals(expected.getTableName(), mtRecord.getTableName());
        assertEquals(expected.getDynamodb().getKeys(), actual.getDynamodb().getKeys());
        assertEquals(expected.getDynamodb().getNewImage(), actual.getDynamodb().getNewImage());
        assertEquals(expected.getDynamodb().getOldImage(), actual.getDynamodb().getOldImage());
    }

    protected static void assertGetRecords(MtAmazonDynamoDbStreams streams, String iterator, MtRecord... expected) {
        GetRecordsResult result = streams.getRecords(new GetRecordsRequest().withShardIterator(iterator));
        assertNotNull(result.getNextShardIterator());
        List<Record> records = result.getRecords();
        assertEquals(expected.length, records.size());
        for (int i = 0; i < expected.length; i++) {
            assertMtRecord(expected[i], records.get(i));
        }
    }

    private static void assertGetRecords(MtAmazonDynamoDbStreams streams, Collection<String> iterators,
        Collection<MtRecord> expected) {
        Function<MtRecord, String> keyFunction =
            record -> record.getContext() + record.getTableName() + record.getDynamodb().getKeys().get(ID_ATTR_NAME)
                .getS();

        Map<String, MtRecord> expectedByKey = expected.stream().collect(toMap(keyFunction, identity()));
        iterators.forEach(iterator ->
            streams.getRecords(new GetRecordsRequest().withShardIterator(iterator)).getRecords().forEach(record -> {
                assertTrue(record instanceof MtRecord);
                MtRecord expectedRecord = expectedByKey.remove(keyFunction.apply((MtRecord) record));
                assertNotNull(expectedRecord);
                assertMtRecord(expectedRecord, record);
            })
        );
        assertTrue(expectedByKey.isEmpty());
    }

    private static class Args implements ArgumentsProvider {

        @Override
        public java.util.stream.Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            String prefix =
                TABLE_PREFIX + context.getTestMethod().orElseThrow(IllegalStateException::new).getName() + ".";

            AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

            MtAmazonDynamoDbBySharedTable indexMtDynamoDb = SharedTableBuilder.builder()
                .withCreateTableRequests(newCreateTableRequest(SHARED_TABLE_NAME))
                .withAmazonDynamoDb(dynamoDb)
                .withTablePrefix(prefix)
                .withContext(MT_CONTEXT)
                .build();

            MtAmazonDynamoDbByTable tableMtDynamoDb = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(prefix)
                .withAmazonDynamoDb(dynamoDb)
                .withContext(MT_CONTEXT)
                .build();

            return java.util.stream.Stream.of(Arguments.of(indexMtDynamoDb), Arguments.of(tableMtDynamoDb));
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
    void testStream(MtAmazonDynamoDbBase mtDynamoDb) {
        final MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
            AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());
        try {
            // create tenant tables and test data
            createTenantTables(mtDynamoDb);
            final Collection<MtRecord> expected = new ArrayList<>(4);
            int i = 0;
            expected.add(putTestItem(mtDynamoDb, TENANTS[0], i++));
            expected.add(putTestItem(mtDynamoDb, TENANTS[0], i));
            i = 0;
            expected.add(putTestItem(mtDynamoDb, TENANTS[1], i++));
            expected.add(putTestItem(mtDynamoDb, TENANTS[1], i));

            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();

            // we are not asserting how many streams we get back, since that's strategy-specific
            List<String> iterators = streams.stream()
                .map(stream -> getShardIterator(mtDynamoDbStreams, stream))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
            // we are also not asserting which stream returns which records, just that we get them all
            assertGetRecords(mtDynamoDbStreams, iterators, expected);
        } finally {
            deleteMtTables(mtDynamoDb);
        }
    }

    /**
     * Verifies that tenant views of tables work as expected.
     */
    @ParameterizedTest
    @ArgumentsSource(Args.class)
    void testTableStream(MtAmazonDynamoDbBase mtDynamoDb) {
        final MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
            AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal());
        try {
            // create tenant tables and test data
            createTenantTables(mtDynamoDb);
            int i = 0;
            final MtRecord expected1 = putTestItem(mtDynamoDb, TENANTS[0], i++);
            final MtRecord expected2 = putTestItem(mtDynamoDb, TENANTS[0], i);
            i = 0;
            putTestItem(mtDynamoDb, TENANTS[1], i++);
            putTestItem(mtDynamoDb, TENANTS[1], i);

            MT_CONTEXT.withContext(TENANTS[0], () -> {
                String streamArn = mtDynamoDb.describeTable(TENANT_TABLE_NAME).getTable().getLatestStreamArn();
                assertNotNull(streamArn);
                String iterator = getShardIterator(mtDynamoDbStreams, streamArn).get();
                assertGetRecords(mtDynamoDbStreams, iterator, expected1, expected2);
            });
        } finally {
            deleteMtTables(mtDynamoDb);
        }
    }

}