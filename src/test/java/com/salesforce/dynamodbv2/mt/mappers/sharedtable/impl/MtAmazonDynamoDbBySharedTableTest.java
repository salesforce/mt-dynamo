package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.ROOT_AMAZON_DYNAMO_DB;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getPollInterval;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtStreamDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable.RecordProcessor;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

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

    @Test
    void listStreams() {
        // create strategy
        MtAmazonDynamoDb amazonDynamoDb = SharedTableBuilder.builder()
            .withPollIntervalSeconds(0)
            .withAmazonDynamoDb(ROOT_AMAZON_DYNAMO_DB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true).build();

        // create table
        MT_CONTEXT.setContext("org1");
        new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
            .createTableIfNotExists(new CreateTableRequest()
                .withTableName(TABLE1)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, HASH)), getPollInterval());

        // prime the cache
        amazonDynamoDb.putItem(new PutItemRequest()
            .withTableName(TABLE1).withItem(ItemBuilder.builder(S, HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE)
                        .build()));

        // list streams
        List<MtStreamDescription> streams = amazonDynamoDb.listStreams(() -> new IRecordProcessor() {
            @Override
            public void initialize(InitializationInput initializationInput) {

            }

            @Override
            public void processRecords(ProcessRecordsInput processRecordsInput) {

            }

            @Override
            public void shutdown(ShutdownInput shutdownInput) {

            }
        });

        // assert
        assertNotNull(streams);
        assertEquals(1, streams.size());
        assertNotNull(streams.get(0).getArn());
    }

    @Test
    void recordProcessor() {
        // create strategy
        MtAmazonDynamoDbBySharedTable amazonDynamoDb = SharedTableBuilder.builder()
            .withPollIntervalSeconds(0)
            .withAmazonDynamoDb(ROOT_AMAZON_DYNAMO_DB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true).build();

        // create table
        String org = "org1";
        MT_CONTEXT.setContext(org);
        new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
            .createTableIfNotExists(new CreateTableRequest()
                .withTableName(TABLE1)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, HASH)), getPollInterval());

        // prime the cache
        amazonDynamoDb.putItem(new PutItemRequest()
            .withTableName(TABLE1).withItem(ItemBuilder.builder(S, HASH_KEY_VALUE)
                        .someField(S, SOME_FIELD_VALUE)
                        .build()));

        // create a record to pass through
        Record record = new Record();
        record.setEventID("eventId");
        record.setEventName("eventName");
        record.setEventSource("eventSource");
        record.setEventVersion("eventVersion");
        StreamRecord streamRecord = new StreamRecord();
        streamRecord.setKeys(ImmutableMap.of("hk", new AttributeValue(org + "." + TABLE1 + ".value")));
        streamRecord.setOldImage(new HashMap<>());
        streamRecord.setNewImage(new HashMap<>());

        // create a record processor that stores the records it receives
        List<com.amazonaws.services.kinesis.model.Record> receivedRecords = new ArrayList<>();
        IRecordProcessor innerRecordProcessor = new IRecordProcessor() {
            @Override
            public void initialize(InitializationInput initializationInput) {

            }

            @Override
            public void processRecords(ProcessRecordsInput processRecordsInput) {
                receivedRecords.addAll(processRecordsInput.getRecords());
            }

            @Override
            public void shutdown(ShutdownInput shutdownInput) {

            }
        };

        // create sut
        RecordProcessor sut = amazonDynamoDb.new RecordProcessor(innerRecordProcessor,
            new DynamoTableDescriptionImpl(new CreateTableRequest()
                .withKeySchema(new KeySchemaElement().withKeyType(HASH).withAttributeName("hk"))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("hk").withAttributeType(S))
                .withProvisionedThroughput(new ProvisionedThroughput())));

        // test
        sut.processRecords(new ProcessRecordsInput()
            .withRecords(ImmutableList.of(new RecordAdapter(record.withDynamodb(streamRecord)))));

        // assert
        assertEquals(1, receivedRecords.size());
        MtRecord mtRecord = (MtRecord) ((RecordAdapter) receivedRecords.get(0)).getInternalObject();
        assertEquals(TABLE1, mtRecord.getTableName());
        assertEquals(record.getEventID(), mtRecord.getEventID());
        assertEquals(record.getEventName(), mtRecord.getEventName());
        assertEquals(record.getEventSource(), mtRecord.getEventSource());
        assertEquals(record.getEventVersion(), mtRecord.getEventVersion());
        assertEquals(org, mtRecord.getContext());
        assertEquals(streamRecord.getKeys(), mtRecord.getDynamodb().getKeys());
        assertEquals(streamRecord.getOldImage(), mtRecord.getDynamodb().getOldImage());
        assertEquals(streamRecord.getNewImage(), mtRecord.getDynamodb().getNewImage());
    }

}