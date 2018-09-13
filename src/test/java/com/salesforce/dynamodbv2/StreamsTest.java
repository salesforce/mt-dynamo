package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.OperationType.INSERT;
import static com.amazonaws.services.dynamodbv2.model.OperationType.MODIFY;
import static com.amazonaws.services.dynamodbv2.model.OperationType.REMOVE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.SOME_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createAttributeValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.createStringAttribute;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getPollInterval;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtStreamDescription;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase;
import com.salesforce.dynamodbv2.mt.mappers.StreamWorker;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.IsolatedArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import com.salesforce.dynamodbv2.testsupport.TestSetup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.awaitility.Duration;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.pollinterval.FixedPollInterval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests streams.
 *
 * @author msgroi
 */
@Tag("isolated-tests")
class StreamsTest {

    private static final AWSCredentialsProvider AWS_CREDENTIALS_PROVIDER = IS_LOCAL_DYNAMO
        ? new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
        : new DefaultAWSCredentialsProviderChain();
    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = ArgumentBuilder.MT_CONTEXT;
    private static final String STREAMS_TABLE = "Streams%sTable";
    private static final String SOME_FIELD_VALUE_STREAMS_TEST = SOME_FIELD_VALUE + "%sStreamsTest";
    private static final String SOME_FIELD_VALUE_STREAMS_TEST_UPDATED = SOME_FIELD_VALUE + "%sStreamsTestUpdated";
    private static AmazonDynamoDB amazonDynamoDb = IsolatedArgumentProvider.getAndInitializeAmazonDynamoDb();
    private static AmazonDynamoDBStreams amazonDynamoDbStreams =
        IsolatedArgumentProvider.getAndInitializeAmazonDynamoDbStreams();
    private static StreamWorker streamWorker;

    @BeforeAll
    static void beforeAll() {
        streamWorker = new StreamWorker(amazonDynamoDb,
            amazonDynamoDbStreams,
            AWS_CREDENTIALS_PROVIDER
        );
    }

    @BeforeEach
    void beforeEach() {
        IsolatedArgumentProvider.getAndInitializeAmazonDynamoDb();
    }

    @AfterEach
    void afterEach() {
        IsolatedArgumentProvider.shutdown();
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(StreamsArgumentProvider.class)
    void test(TestArgument testArgument) {
        // expected records
        List<MtRecord> expectedRecords = getExpectedMtRecords(testArgument.getOrgs(),
            testArgument.getHashKeyAttrType());

        // get orgs
        List<String> orgs = testArgument.getOrgs();

        // for each org, create a table and put a record
        orgs.forEach(org -> {
            MT_CONTEXT.setContext(org);
            new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                .createTableIfNotExists(new CreateTableRequest()
                    .withTableName(format(STREAMS_TABLE, org))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD,
                        testArgument.getHashKeyAttrType()))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)), getPollInterval());
            testArgument.getAmazonDynamoDb().putItem(new PutItemRequest()
                .withTableName(format(STREAMS_TABLE, org))
                .withItem(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .someField(S, format(SOME_FIELD_VALUE_STREAMS_TEST, org))
                        .build()));
        });

        // for each org, update the record
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            testArgument.getAmazonDynamoDb().updateItem(new UpdateItemRequest()
                .withTableName(format(STREAMS_TABLE, org))
                .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                        .build())
                .addAttributeUpdatesEntry(SOME_FIELD,
                    new AttributeValueUpdate().withValue(createStringAttribute(format(
                        SOME_FIELD_VALUE_STREAMS_TEST_UPDATED, org)))));
        });

        // for each org, delete the record
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            testArgument.getAmazonDynamoDb().deleteItem(
                new DeleteItemRequest().withTableName(format(STREAMS_TABLE, org))
                    .withKey(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                            .build()));
        });

        // create record processor
        RecordProcessor recordProcessor = new RecordProcessor();

        // start a worker per stream arn
        ((MtAmazonDynamoDbBase) testArgument.getAmazonDynamoDb()).listStreams(() -> recordProcessor).stream()
            .collect(collectingAndThen(toCollection(() -> new TreeSet<>(
                Comparator.comparing(MtStreamDescription::getArn))), ArrayList::new)).forEach(streamWorker::start);

        // wait for the records to arrive
        try {
            await().pollInSameThread()
                .pollInterval(new FixedPollInterval(new Duration(1, SECONDS)))
                .atMost(40, SECONDS)
                .until(() -> recordProcessor.mtRecords.size() >= expectedRecords.size());

            // assert records received
            assertRecordsReceived(expectedRecords, recordProcessor.mtRecords);
        } catch (ConditionTimeoutException e) {
            fail("timeout, " + recordProcessor.mtRecords.size()
                + " records arrived, expected " + expectedRecords.size());
        }

        streamWorker.stop();
    }

    private void assertRecordsReceived(List<MtRecord> expectedRecords, List<MtRecord> actualRecords) {
        assertEquals(expectedRecords.size(), actualRecords.size(),
            actualRecords.size() + " of " + expectedRecords.size() + " records received, actual=" + actualRecords);
        for (MtRecord recordReceived : actualRecords) {
            assertMtRecord(recordReceived, expectedRecords);
        }
        assertEquals(0, expectedRecords.size(), "records not encountered: " + expectedRecords);
    }

    private void assertMtRecord(MtRecord receivedRecord, List<MtRecord> expectedRecords) {
        Optional<MtRecord> mtRecordFound = expectedRecords
            .stream()
            .filter(mtRecord -> weakMtRecordEquals(mtRecord, receivedRecord))
            .findFirst();
        if (mtRecordFound.isPresent()) {
            expectedRecords.remove(mtRecordFound.get());
        } else {
            throw new IllegalArgumentException("unexpected MtRecord encountered: " + receivedRecord);
        }
    }

    private boolean weakMtRecordEquals(MtRecord r1, MtRecord r2) {
        try {
            return Objects.equal(r1.getContext(), r2.getContext())
                && Objects.equal(r1.getTableName(), r2.getTableName())
                && Objects.equal(r1.getEventName(), r2.getEventName())
                && Objects.equal(r1.getDynamodb().getKeys(), r2.getDynamodb().getKeys())
                && Objects.equal(r1.getDynamodb().getOldImage(), r2.getDynamodb().getOldImage())
                && Objects.equal(r1.getDynamodb().getNewImage(), r2.getDynamodb().getNewImage());
        } catch (Exception e) {
            throw new RuntimeException(e.getClass() + " comparing record ... " + r1 + " to record ... " + r2);
        }
    }

    private List<MtRecord> getExpectedMtRecords(List<String> orgs, ScalarAttributeType hashKeyAttrType) {
        List<MtRecord> inserts = orgs.stream().map(org -> new MtRecord().withContext(org)
            .withTableName(format(STREAMS_TABLE, org))
            .withEventName(INSERT.name())
            .withDynamodb(new StreamRecord()
                .withKeys(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE)))
                .withNewImage(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE),
                    SOME_FIELD, new AttributeValue().withS(format(SOME_FIELD_VALUE_STREAMS_TEST, org))))))
            .collect(Collectors.toList());
        List<MtRecord> updates = orgs.stream().map(org -> new MtRecord().withContext(org)
            .withTableName(format(STREAMS_TABLE, org))
            .withEventName(MODIFY.name())
            .withDynamodb(new StreamRecord()
                .withKeys(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE)))
                .withOldImage(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE),
                    SOME_FIELD, new AttributeValue().withS(format(SOME_FIELD_VALUE_STREAMS_TEST, org))))
                .withNewImage(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE),
                    SOME_FIELD, new AttributeValue().withS(format(SOME_FIELD_VALUE_STREAMS_TEST_UPDATED, org))))))
            .collect(Collectors.toList());
        List<MtRecord> deletes = orgs.stream().map(org -> new MtRecord().withContext(org)
            .withTableName(format(STREAMS_TABLE, org))
            .withEventName(REMOVE.name())
            .withDynamodb(new StreamRecord()
                .withKeys(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE)))
                .withOldImage(ImmutableMap.of(HASH_KEY_FIELD, createAttributeValue(hashKeyAttrType, HASH_KEY_VALUE),
                    SOME_FIELD, new AttributeValue().withS(format(SOME_FIELD_VALUE_STREAMS_TEST_UPDATED, org))))))
            .collect(Collectors.toList());
        return Stream.of(
            inserts,
            updates,
            deletes
        ).flatMap(Collection::stream).collect(Collectors.toList());
    }

    private static class RecordProcessor implements IRecordProcessor {
        List<MtRecord> mtRecords = new ArrayList<>();

        @Override
        public void initialize(InitializationInput initializationInput) {
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            processRecordsInput.getRecords().forEach(record ->
                mtRecords.add((MtRecord) ((RecordAdapter) record).getInternalObject()));
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
        }
    }

    /*
     * Uses the isolated argument provider to get a standalone DynamoDB instance and override the default setup
     * to be a no-op.
     */
    private static class StreamsArgumentProvider extends IsolatedArgumentProvider {

        StreamsArgumentProvider() {
            super(new TestSetup() {
                @Override
                public void setupTest(TestArgument testArgument) {
                }

                @Override
                public void setupTableData(AmazonDynamoDB amazonDynamoDb, ScalarAttributeType hashKeyAttrType,
                    String org, CreateTableRequest createTableRequest) {
                }
            });
        }

    }

}
