package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.function.Function.identity;
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
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams.MtGetRecordsResult;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Assumptions;

public class MtAmazonDynamoDbStreamsBaseTestUtils {

    public static final String SHARED_TABLE_NAME = "SharedTable";
    public static final String[] TENANTS = { "tenant1", "tenant2" };
    public static final String TENANT_TABLE_NAME = "TenantTable";
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    /**
     * Test utility method.
     */
    public static CreateTableRequest newCreateTableRequest(String tableName, boolean binaryHashKey) {
        return new CreateTableRequest()
            .withTableName(tableName + (binaryHashKey ? "_b" : ""))
            .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
            .withAttributeDefinitions(
                new AttributeDefinition(ID_ATTR_NAME, binaryHashKey ? B : S),
                new AttributeDefinition(INDEX_ID_ATTR_NAME, binaryHashKey ? B : S))
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

    public static CreateTableRequest newHashPartitioningCreateTableRequest(String tableName) {
        return new CreateTableRequest()
            .withTableName(tableName + "_HashPartitioning")
            .withKeySchema(new KeySchemaElement("physicalHk", HASH), new KeySchemaElement("physicalRk", RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition("physicalHk", B),
                new AttributeDefinition("physicalRk", B),
                new AttributeDefinition("physicalGsiHk", B),
                new AttributeDefinition("physicalGsiRk", B))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                .withIndexName("physicalGsi")
                .withKeySchema(new KeySchemaElement("physicalGsiHk", HASH),
                    new KeySchemaElement("physicalGsiRk", RANGE))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            )
            .withStreamSpecification(new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(NEW_AND_OLD_IMAGES));
    }

    /**
     * Test utility method.
     */
    public static void createTenantTables(AmazonDynamoDB mtDynamoDb) {
        for (String tenant : TENANTS) {
            MT_CONTEXT.withContext(tenant, () -> mtDynamoDb.createTable(newCreateTableRequest(
                TENANT_TABLE_NAME, false)));
        }
    }

    /**
     * Test utility method.
     */
    public static void deleteMtTables(MtAmazonDynamoDbBase mtDynamoDb) {
        mtDynamoDb.listTables().getTableNames().stream().filter(mtDynamoDb::isMtTable).forEach(
            name -> TableUtils.deleteTableIfExists(mtDynamoDb.getAmazonDynamoDb(),
                new DeleteTableRequest(name)));
    }

    public static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams,
                                                    AmazonDynamoDB mtDynamoDb) {
        return getShardIterator(mtDynamoDbStreams,
            mtDynamoDb.describeTable(TENANT_TABLE_NAME).getTable().getLatestStreamArn());
    }

    public static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams, Stream stream) {
        return getShardIterator(mtDynamoDbStreams, stream.getStreamArn());
    }

    static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams, String streamArn) {
        return getShardIterator(mtDynamoDbStreams, streamArn, ShardIteratorType.TRIM_HORIZON, null);
    }

    static Optional<String> getShardIterator(AmazonDynamoDBStreams mtDynamoDbStreams, String streamArn,
                                             ShardIteratorType type, String sequenceNumber) {
        return getShardId(mtDynamoDbStreams, streamArn).map(shardId ->
            mtDynamoDbStreams.getShardIterator(new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(type)
                .withSequenceNumber(sequenceNumber)).getShardIterator()
        );
    }

    static Optional<String> getShardId(AmazonDynamoDBStreams mtDynamoDbStreams, String streamArn) {
        DescribeStreamResult dsResult = mtDynamoDbStreams.describeStream(
            new DescribeStreamRequest().withStreamArn(streamArn));

        if (StreamStatus.fromValue(dsResult.getStreamDescription().getStreamStatus()) != StreamStatus.ENABLED) {
            return Optional.empty();
        }

        // assumes we are running against local MT dynamo that has one shard per stream
        Assumptions.assumeTrue(dsResult.getStreamDescription().getShards().size() == 1);
        return Optional.of(dsResult.getStreamDescription().getShards().get(0).getShardId());
    }

    /**
     * Puts an item into the table in the given tenant context for the given {@code id} and returns the expected MT
     * record.
     */
    public static MtRecord putTestItem(AmazonDynamoDB dynamoDb, String tenant, int id) {
        return putTestItem(dynamoDb, TENANT_TABLE_NAME, tenant, id);
    }

    static MtRecord putTestItem(AmazonDynamoDB dynamoDb, String table, String tenant, int id) {
        return MT_CONTEXT.withContext(tenant, sid -> {
            PutItemRequest put = new PutItemRequest(table, ImmutableMap.of(
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

    static boolean equals(MtRecord expected, Record actual) {
        if (!(actual instanceof MtRecord)) {
            return false;
        }
        final MtRecord mtRecord = (MtRecord) actual;
        boolean equals = Objects.equals(expected.getContext(), mtRecord.getContext())
            && Objects.equals(expected.getTableName(), mtRecord.getTableName());
        /*
         * This should happen temporarily while we do not properly process delete markers, so keys, and old/new
         * image are not properly mapped over on a deleted virtual table, as we lost the ability to
         * do that mapping.
         */
        if (mtRecord.getContext() != null) {
            equals &= Objects.equals(expected.getDynamodb().getKeys(), actual.getDynamodb().getKeys())
                && Objects.equals(expected.getDynamodb().getNewImage(), actual.getDynamodb().getNewImage())
                && Objects.equals(expected.getDynamodb().getOldImage(), actual.getDynamodb().getOldImage());
        }
        return equals;
    }

    public static void assertMtRecord(MtRecord expected, Record actual) {
        assertTrue(equals(expected, actual));
    }

    public static void assertGetRecords(MtAmazonDynamoDbStreams streams, String iterator, MtRecord... expected) {
        assertGetRecords(streams, iterator, null, expected);
    }

    static String assertGetRecords(MtAmazonDynamoDbStreams streams, String iterator, Integer limit,
                                   MtRecord... expected) {
        GetRecordsResult result = streams
            .getRecords(new GetRecordsRequest().withShardIterator(iterator).withLimit(limit));
        assertNotNull(result.getNextShardIterator());
        List<Record> records = result.getRecords();
        assertEquals(expected.length, records.size());
        for (int i = 0; i < expected.length; i++) {
            assertMtRecord(expected[i], records.get(i));
        }
        return records.isEmpty() ? null : Iterables.getLast(records).getDynamodb().getSequenceNumber();
    }

    static void assertGetRecords(MtAmazonDynamoDbStreams streams, Collection<String> iterators,
                                 Collection<MtRecord> expected) {
        Function<MtRecord, String> keyFunction =
            record -> record.getContext() + record.getTableName() + record.getDynamodb().getKeys().get(ID_ATTR_NAME)
                .getS();

        Map<String, MtRecord> expectedByKey = expected.stream().collect(toMap(keyFunction, identity()));
        iterators.forEach(iterator -> {
            GetRecordsResult result = streams.getRecords(new GetRecordsRequest().withShardIterator(iterator));
            assertTrue(result instanceof MtGetRecordsResult);
            assertNotNull(((MtGetRecordsResult) result).getLastSequenceNumber());
            result.getRecords().forEach(record -> {
                assertTrue(record instanceof MtRecord);
                MtRecord expectedRecord = expectedByKey.remove(keyFunction.apply((MtRecord) record));
                assertNotNull(expectedRecord);
                assertMtRecord(expectedRecord, record);
            });
        });
        assertTrue(expectedByKey.isEmpty());
    }
}
