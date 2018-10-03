package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_IMAGE;
import static com.google.common.collect.Iterables.getLast;
import static com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams.GET_RECORDS_LIMIT;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams.Sleeper;
import com.salesforce.dynamodbv2.testsupport.CountingAmazonDynamoDbStreams;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

/**
 * Tests the caching streams adapter.
 */
class CachingAmazonDynamoDbStreamsTest {

    private static Level level;

    @BeforeAll
    static void beforeClass() {
        Logger logger = (Logger) LoggerFactory.getLogger(CachingAmazonDynamoDbStreams.class);
        level = logger.getLevel();
        logger.setLevel(Level.DEBUG);
    }

    @AfterAll
    static void afterClass() {
        ((Logger) LoggerFactory.getLogger(CachingAmazonDynamoDbStreams.class)).setLevel(level);
    }

    /**
     * Test runs against actual DynamoDB (local, but should support remote as well), simulates multiple clients reading
     * the stream at different offsets, and verifies that cache is used to service those requests as expected. The
     * finer-grained verification of various binning and race conditions are tested separately against mock streams,
     * since that's easier to control.
     */
    @Test
    void integrationTest() throws InterruptedException {
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        AmazonDynamoDBStreams dynamoDbStreams = AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal();
        CountingAmazonDynamoDbStreams countingDynamoDbStreams = new CountingAmazonDynamoDbStreams(dynamoDbStreams);
        CachingAmazonDynamoDbStreams cachingDynamoDbStreams = new CachingAmazonDynamoDbStreams.Builder(
            countingDynamoDbStreams).build();

        // setup: create a table with streams enabled
        String tableName = CachingAmazonDynamoDbStreamsTest.class.getSimpleName() + "_it_" + System.currentTimeMillis();
        String pk = "id";
        TableDescription tableDescription = dynamoDb.createTable(new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(new AttributeDefinition(pk, S))
            .withKeySchema(new KeySchemaElement(pk, HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withStreamSpecification(new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(NEW_IMAGE))).getTableDescription();
        try {
            TableUtils.waitUntilActive(dynamoDb, tableName);

            String streamArn = tableDescription.getLatestStreamArn();
            StreamDescription streamDescription = cachingDynamoDbStreams.describeStream(
                new DescribeStreamRequest().withStreamArn(streamArn)).getStreamDescription();
            assertEquals(StreamStatus.ENABLED.toString(), streamDescription.getStreamStatus());

            List<Shard> shardIds = streamDescription.getShards();
            assertEquals(1, shardIds.size());
            Shard shard = shardIds.get(0);
            assertNull(shard.getParentShardId());
            assertNull(shard.getSequenceNumberRange().getEndingSequenceNumber());
            String shardId = shard.getShardId();

            //  now insert records (two pages worth)
            for (int i = 0; i < 2 * GET_RECORDS_LIMIT; i++) {
                dynamoDb.putItem(tableName, ImmutableMap.of(pk, new AttributeValue(String.valueOf(i))));
            }

            // first client fetches records starting at the trim horizon with a limit that's smaller than page size
            String iterator = cachingDynamoDbStreams.getShardIterator(new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)).getShardIterator();
            int limit = 100;
            GetRecordsResult result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(iterator)
                .withLimit(limit));
            String nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            List<Record> records = result.getRecords();
            assertEquals(limit, records.size());
            assertEquals(1, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

            // first client now makes another request that fetches the remaining records of the first page
            limit = GET_RECORDS_LIMIT - limit;
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIterator)
                .withLimit(limit));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(limit, records.size());
            // the result should have been completely served from cache
            assertEquals(1, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);
            final Record lastRecord = getLast(records);

            // second client fetches records starting at trim horizon with limit smaller than page size, but larger
            // than initial limit of first client
            iterator = cachingDynamoDbStreams.getShardIterator(new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)).getShardIterator();
            limit = 600;
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(iterator)
                .withLimit(limit));
            String nextShardIteratorClient2 = result.getNextShardIterator();
            assertNotNull(nextShardIteratorClient2);
            records = result.getRecords();
            assertEquals(limit, records.size());
            // trim horizon results are not serviced from cache
            assertEquals(2, countingDynamoDbStreams.getRecordsCount);
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);

            // second client fetches next range, which extends beyond first fetched page
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIteratorClient2)
                .withLimit(limit));
            nextShardIteratorClient2 = result.getNextShardIterator();
            assertNotNull(nextShardIteratorClient2);
            records = result.getRecords();
            // we expect that we'll end at the current cache segment boundary
            assertEquals(GET_RECORDS_LIMIT - limit, records.size());
            assertEquals(lastRecord, getLast(records));

            // second client moves onto next page
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIteratorClient2)
                .withLimit(limit));
            nextShardIteratorClient2 = result.getNextShardIterator();
            assertNotNull(nextShardIteratorClient2);
            records = result.getRecords();
            assertEquals(limit, records.size());
            // the records count should now be 3, since we fetched the second page
            assertEquals(3, countingDynamoDbStreams.getRecordsCount);
            // the shard iterator count should still be 2, since we cached 'next iterator' of first page
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);

            // second client retrieves remaining chunk of second page
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIteratorClient2)
                .withLimit(limit));
            nextShardIteratorClient2 = result.getNextShardIterator();
            assertNotNull(nextShardIteratorClient2);
            records = result.getRecords();
            assertEquals(GET_RECORDS_LIMIT - limit, records.size());
            // the records count should still be 3, since we fetched the second page
            assertEquals(3, countingDynamoDbStreams.getRecordsCount);
            // the shard iterator count should still be 2, since we cached 'next iterator' of first page
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);

            // first client now fetches second page (without limit)
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIterator));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(GET_RECORDS_LIMIT, records.size());
            assertEquals(3, countingDynamoDbStreams.getRecordsCount);
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);

            // first client now tries to go beyond second page which has no records yet
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIterator));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(0, records.size());
            // another getRecords call to fetch empty page
            assertEquals(4, countingDynamoDbStreams.getRecordsCount);
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);

            // second client now tries to go beyond second page which still has no records
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                .withShardIterator(nextShardIteratorClient2));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(0, records.size());
            // another getRecords call to fetch empty page
            assertEquals(5, countingDynamoDbStreams.getRecordsCount);
            assertEquals(2, countingDynamoDbStreams.getShardIteratorCount);
        } finally {
            // cleanup after ourselves (want to be able to run against hosted DynamoDB as well)
            dynamoDb.deleteTable(tableName);
        }
    }

    // some test fixtures
    private static final String streamArn = "stream1";
    private static final String shardId = "shard1";
    private static final List<Record> records = IntStream.range(0, 10)
        .map(i -> i * 10) // multiples of 10 to simulate non-contiguous nature
        .mapToObj(CachingAmazonDynamoDbStreamsTest::mockRecord)
        .collect(toList());

    private static String formatSequenceNumber(int sequenceNumber) {
        return String.format("%021d", sequenceNumber);
    }

    private static Record mockRecord(int sequenceNumber) {
        return new Record()
            .withEventID(UUID.randomUUID().toString())
            .withEventSource("aws:dynamodb")
            .withEventName(OperationType.INSERT)
            .withEventVersion("1.1")
            .withAwsRegion("ddblocal")
            .withDynamodb(new StreamRecord()
                .withSequenceNumber(formatSequenceNumber(sequenceNumber))
                .withSizeBytes(1L)
            );
    }

    private static String getMockRecordSequenceNumber(int idx) {
        return records.get(idx).getDynamodb().getSequenceNumber();
    }

    private static String getMockRecordSequenceNumberPlusOne(int index) {
        String sequenceNumber = getMockRecordSequenceNumber(index);
        return String.valueOf(Integer.parseInt(sequenceNumber) + 1);
    }

    private static String mockShardIterator(GetShardIteratorRequest iteratorRequest) {
        return String.join(
            "/",
            "mock-shard-iterator" + UUID.randomUUID(),
            iteratorRequest.getStreamArn(),
            iteratorRequest.getShardId(),
            iteratorRequest.getShardIteratorType(),
            iteratorRequest.getSequenceNumber());
    }

    private static GetShardIteratorRequest newTrimHorizonRequest() {
        return new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
    }

    private static GetShardIteratorRequest newAfterSequenceNumberRequest(int index) {
        return new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
            .withSequenceNumber(getMockRecordSequenceNumber(index));
    }

    private static GetShardIteratorRequest newAtSequenceNumberRequest(int index) {
        return new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
            .withSequenceNumber(getMockRecordSequenceNumber(index));
    }

    private static void mockGetShardIterator(AmazonDynamoDBStreams streams, GetShardIteratorRequest iteratorRequest,
        String iterator) {
        GetShardIteratorResult result = new GetShardIteratorResult().withShardIterator(iterator);
        when(streams.getShardIterator(eq(iteratorRequest))).thenReturn(result);
    }

    private static String mockGetShardIterator(AmazonDynamoDBStreams streams, GetShardIteratorRequest iteratorRequest) {
        String iterator = mockShardIterator(iteratorRequest);
        mockGetShardIterator(streams, iteratorRequest, iterator);
        return iterator;
    }

    private static void mockGetRecords(AmazonDynamoDBStreams streams, String iterator, List<Record>
        records, String nextIterator) {
        GetRecordsRequest request = new GetRecordsRequest().withShardIterator(iterator);
        GetRecordsResult result = new GetRecordsResult().withRecords(records).withNextShardIterator(nextIterator);
        when(streams.getRecords(eq(request))).thenReturn(result);
    }

    private static String mockGetRecords(AmazonDynamoDBStreams streams, String iterator, int from, int to) {
        List<Record> mockRecords = records.subList(from, to);
        String nextIterator = to < records.size() ? mockShardIterator(newAfterSequenceNumberRequest(to - 1)) : null;
        mockGetRecords(streams, iterator, mockRecords, nextIterator);
        return nextIterator;
    }

    private static String mockGetAllRecords(AmazonDynamoDBStreams streams, String iterator) {
        mockGetRecords(streams, iterator, records, null);
        return null;
    }

    private static String assertGetRecords(AmazonDynamoDBStreams
        streams,
        String iterator,
        Integer limit,
        List<Record> expectedRecords) {
        GetRecordsRequest request = new GetRecordsRequest().withShardIterator(iterator).withLimit(limit);
        GetRecordsResult result = streams.getRecords(request);
        assertEquals(expectedRecords, result.getRecords());
        return result.getNextShardIterator();
    }

    private static String assertGetRecords(AmazonDynamoDBStreams streams,
        GetShardIteratorRequest iteratorRequest,
        Integer limit,
        int from,
        int to) {
        final String iterator = streams.getShardIterator(iteratorRequest).getShardIterator();
        assertNotNull(iterator);
        final List<Record> mockRecords = records.subList(from, to);
        return assertGetRecords(streams, iterator, limit, mockRecords);
    }

    private static GetShardIteratorRequest mockTrimHorizonRequest(AmazonDynamoDBStreams streams,
        String streamArn,
        String shardId) {
        GetShardIteratorRequest thRequest = new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        String iterator = mockGetShardIterator(streams, thRequest);
        mockGetAllRecords(streams, iterator);
        return thRequest;
    }

    private static CachingAmazonDynamoDbStreams mockTrimHorizonStream(AmazonDynamoDBStreams streams) {
        GetShardIteratorRequest iteratorRequest = mockTrimHorizonRequest(streams, streamArn, shardId);

        // get exact overlapping records with and without limit
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        assertGetRecords(cachingStreams, iteratorRequest, null, 0, 10);

        assertCacheMisses(streams, 1, 1);

        return cachingStreams;
    }

    private static void assertCacheMisses(AmazonDynamoDBStreams streams, int numGetShardIterators, int numGetRecords) {
        verify(streams, times(numGetShardIterators)).getShardIterator(any());
        verify(streams, times(numGetRecords)).getRecords(any());
    }

    /**
     * Verifies that invalid sequence numbers are rejected.
     */
    @Test
    void testInvalidSequenceNumbers() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // no sequence number
        assertThrows(IllegalArgumentException.class,
            () -> cachingStreams.getShardIterator(new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)));

        // invalid sequence number (not a number)
        assertThrows(IllegalArgumentException.class,
            () -> cachingStreams.getShardIterator(new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .withSequenceNumber("a")));
    }

    /**
     * Verifies that {@link ShardIteratorType#TRIM_HORIZON} requests do not cache iterators or records.
     */
    @Test
    void testTrimHorizonNotCached() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newTrimHorizonRequest(), null, 0, 10);

        // verify that underlying stream was still accessed for each request
        assertCacheMisses(streams, 2, 2);
    }

    /**
     * Verifies that an exact cache hit can be serviced completely from the cache.
     */
    @Test
    void testExactCacheHit() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newAtSequenceNumberRequest(0), null, 0, 10);

        // verify that underlying stream was still accessed only once
        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that exact cache hit with limit can be completely serviced from cache.
     */
    @Test
    void testExactCacheHitWithLimit() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newAtSequenceNumberRequest(0), 5, 0, 5);

        // verify that underlying stream was still accessed only once
        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that a partial cache hit can still be serviced from the cache, but returns less.
     */
    @Test
    void testPartialCacheHit() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newAfterSequenceNumberRequest(4), 10, 5, 10);

        // verify that underlying stream was still accessed only once
        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that {@link ShardIteratorType#AT_SEQUENCE_NUMBER} is serviced from same cache.
     */
    @Test
    void testAtSequence() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newAtSequenceNumberRequest(5), 10, 5, 10);

        // verify that underlying stream was still accessed only once
        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that if a new cache segment is added that overlaps with the next segment, the segment is aligned such
     * that overlapping records are not cached twice.
     */
    @Test
    void testOverlappingUpdate() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest afterSnIteratorRequest = newAfterSequenceNumberRequest(4);
        String afterSnIterator = mockGetShardIterator(streams, afterSnIteratorRequest);
        mockGetRecords(streams, afterSnIterator, 5, 10);

        GetShardIteratorRequest trimHorizonIteratorRequest = newTrimHorizonRequest();
        String trimHorizonIterator = mockGetShardIterator(streams, trimHorizonIteratorRequest);
        mockGetRecords(streams, trimHorizonIterator, 0, 7);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // test

        // first get records after sequence number, then get records from trim horizon (before)
        assertGetRecords(cachingStreams, afterSnIteratorRequest, null, 5, 10);
        assertGetRecords(cachingStreams, trimHorizonIteratorRequest, null, 0, 10);
        assertCacheMisses(streams, 2, 2);

        // now retrieve a page and make sure requests serviced from cache (implying ranges were merged)
        assertNull(assertGetRecords(cachingStreams, newAfterSequenceNumberRequest(2), null, 3, 10));
        assertCacheMisses(streams, 2, 2);
    }

    /**
     * Verifies that if a new cache segment is added that overlaps completely with the next, i.e., does not contain any
     * records that are not already in the next, but uses a different shard iterator, the next segment is merged with
     * the new one, meaning all its records are cached using the new shard iterator key and are still accessible via
     * both iterators.
     */
    @Test
    void testCompleteOverlap() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest afterSnIteratorRequest = newAfterSequenceNumberRequest(5)
            .withSequenceNumber(getMockRecordSequenceNumberPlusOne(5)); // start between records
        String afterSnIterator = mockGetShardIterator(streams, afterSnIteratorRequest);
        mockGetRecords(streams, afterSnIterator, 6, 9);

        GetShardIteratorRequest afterRecordIteratorRequest = newAfterSequenceNumberRequest(5);
        String afterRecordIterator = mockGetShardIterator(streams, afterRecordIteratorRequest);
        mockGetRecords(streams, afterRecordIterator, 6, 10);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // first make record requests that starts at higher offset
        assertGetRecords(cachingStreams, afterSnIteratorRequest, null, 6, 9);

        // now make record request that starts at lower offset, but returns same records
        assertGetRecords(cachingStreams, afterRecordIteratorRequest, null, 6, 9);

        assertCacheMisses(streams, 2, 2);

        // now if we retrieve either segment again, it will be from cache
        assertGetRecords(cachingStreams, afterSnIteratorRequest, null, 6, 9);
        assertGetRecords(cachingStreams, afterRecordIteratorRequest, null, 6, 9);

        assertCacheMisses(streams, 2, 2);
    }

    /**
     * Verifies that if a gap is closed, adjacent segments are merged. Example here:
     * <ol>
     * <li>Retrieve 5 records starting at trim horizon: creates segment [0, 40].</li>
     * <li>Retrieve 5 records starting with 50: creates segment [50, 90].</li>
     * <li>Retrieve records starting at 41: detects no more records between 40 and 50 and merges into [0, 90]</li>
     * </ol>
     */
    @Test
    void testCloseGap() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thRequest);
        mockGetRecords(streams, thIterator, 0, 5);

        GetShardIteratorRequest atRequest = newAtSequenceNumberRequest(5);
        String atIterator = mockGetShardIterator(streams, atRequest);
        mockGetRecords(streams, atIterator, 5, 10);

        GetShardIteratorRequest afterRequest = newAfterSequenceNumberRequest(4);
        String afterIterator = mockGetShardIterator(streams, afterRequest);
        mockGetRecords(streams, afterIterator, 5, 10);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        assertGetRecords(cachingStreams, thRequest, null, 0, 5);
        assertGetRecords(cachingStreams, atRequest, null, 5, 10);
        assertGetRecords(cachingStreams, afterRequest, null, 5, 10);
        assertGetRecords(cachingStreams, newAtSequenceNumberRequest(0), null, 0, 10);

        assertCacheMisses(streams, 3, 3);
    }

    /**
     * Verify that we correctly handle an empty page at the end of a stream.
     */
    @Test
    void testEmptyTerminate() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thIteratorRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thIteratorRequest);
        GetShardIteratorRequest afterEndRequest = newAfterSequenceNumberRequest(9);
        String afterEndIterator = mockGetShardIterator(streams, afterEndRequest);
        mockGetRecords(streams, thIterator, records, afterEndIterator);
        mockGetRecords(streams, afterEndIterator, Collections.emptyList(), null);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // test
        String next = assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 10);
        assertNull(assertGetRecords(cachingStreams, next, null, Collections.emptyList()));

        assertCacheMisses(streams, 1, 2);
    }

    /**
     * Test that limit exceeded exceptions are retried and still cached.
     */
    @Test
    void testRetry() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thIteratorRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thIteratorRequest);
        GetRecordsRequest request = new GetRecordsRequest().withShardIterator(thIterator);

        when(streams.getRecords(request))
            .thenThrow(new LimitExceededException("High contention"))
            .thenReturn(new GetRecordsResult().withRecords(records).withNextShardIterator(null));

        Sleeper sleeper = mock(Sleeper.class);
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
            .withGetRecordsLimitExceededBackoffInMillis(10000L)
            .withSleeper(sleeper)
            .build();

        assertNull(assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 10));
        assertNull(assertGetRecords(cachingStreams, newAtSequenceNumberRequest(0), null, 0, 10));

        // trim horizon shard iterators should not be cached, so 2 calls expected
        assertCacheMisses(streams, 1, 2);

        // one backoff with specified interval
        verify(sleeper, times(1)).sleep(eq(10000L));
    }

    /**
     * Verifies that retry backs off appropriately and eventually fails when limit is exceeded.
     */
    @Test
    void testRetryLimitExceeded() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thIteratorRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thIteratorRequest);
        GetRecordsRequest request = new GetRecordsRequest().withShardIterator(thIterator);
        when(streams.getRecords(request)).thenThrow(new LimitExceededException(""));

        Sleeper sleeper = mock(Sleeper.class);
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
            .withGetRecordsLimitExceededBackoffInMillis(500)
            .withMaxGetRecordsRetries(3)
            .withSleeper(sleeper)
            .withMaxRecordsByteSize(100L)
            .build();

        final String iterator = cachingStreams.getShardIterator(thIteratorRequest).getShardIterator();
        try {
            cachingStreams.getRecords(new GetRecordsRequest().withShardIterator(iterator));
            fail("Expected limit exceeded exception");
        } catch (LimitExceededException e) {
            // expected
        }

        // trim horizon shard iterator not cached, so 3 expected
        assertCacheMisses(streams, 1, 3);

        ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
        verify(sleeper, times(3)).sleep(captor.capture());
        assertEquals(Arrays.asList(500L, 1000L, 1500L), captor.getAllValues());
    }

    /**
     * Verifies that iterator expiration is handled correctly (and not counted toward retries).
     */
    @Test
    void testIteratorExpiration() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        // mock iterators: expect two calls
        GetShardIteratorRequest iteratorRequest = newTrimHorizonRequest();
        String iterator = mockGetShardIterator(streams, iteratorRequest);

        // calling with expired iterator fails
        when(streams.getRecords(eq(new GetRecordsRequest().withShardIterator(iterator))))
            .thenThrow(new ExpiredIteratorException(""));

        Sleeper sleeper = mock(Sleeper.class);
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
            .withMaxGetRecordsRetries(1)
            .withSleeper(sleeper)
            .build();

        String logicalIterator = cachingStreams.getShardIterator(iteratorRequest).getShardIterator();
        assertThrows(ExpiredIteratorException.class,
            () -> cachingStreams.getRecords(new GetRecordsRequest().withShardIterator(logicalIterator)));

        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that records and iterators are evicted from caches per specified max sizes.
     */
    @Test
    void testCacheEviction() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest firstRequest = newAtSequenceNumberRequest(1);
        String firstIterator = mockGetShardIterator(streams, firstRequest);
        mockGetRecords(streams, firstIterator, 0, 4);

        GetShardIteratorRequest secondRequest = newAtSequenceNumberRequest(5);
        String secondIterator = mockGetShardIterator(streams, secondRequest);
        mockGetRecords(streams, secondIterator, 5, 10);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
            .withMaxRecordsByteSize(1L)
            .build();

        assertGetRecords(cachingStreams, firstRequest, null, 0, 4);
        // nothing cached, so both should be misses
        assertCacheMisses(streams, 1, 1);

        assertGetRecords(cachingStreams, firstRequest, null, 0, 4);
        // records are now cached, so no need to load records or iterator
        assertCacheMisses(streams, 1, 1);

        assertGetRecords(cachingStreams, secondRequest, null, 5, 10);
        // new segment not cached, so need to load records and iterator
        assertCacheMisses(streams, 2, 2);

        assertGetRecords(cachingStreams, secondRequest, null, 5, 10);
        // new segment still cached, so no need to load records and iterator
        assertCacheMisses(streams, 2, 2);

        assertGetRecords(cachingStreams, firstRequest, null, 0, 4);
        // first segment should have been evicted on previous round, so new loads needed
        assertCacheMisses(streams, 3, 3);
    }

    /**
     * Verifies that cache properly separates streams and shards.
     */
    @Test
    void testMultipleShards() {
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        final GetShardIteratorRequest thRequest11 = mockTrimHorizonRequest(streams, "stream1", "shard1");
        final GetShardIteratorRequest thRequest12 = mockTrimHorizonRequest(streams, "stream1", "shard2");
        final GetShardIteratorRequest thRequest21 = mockTrimHorizonRequest(streams, "stream2", "shard1");
        final GetShardIteratorRequest thRequest22 = mockTrimHorizonRequest(streams, "stream2", "shard2");

        final CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        assertGetRecords(cachingStreams, thRequest11, null, 0, 10);
        assertGetRecords(cachingStreams, thRequest12, null, 0, 10);
        assertGetRecords(cachingStreams, thRequest21, null, 0, 10);
        assertGetRecords(cachingStreams, thRequest22, null, 0, 10);

        assertCacheMisses(streams, 4, 4);
    }

    /**
     * Verifies that TRIM_HORIZON pointer is not cached. Specifically, it simulates the scenario that the initial
     * iterator returned by getShardIterator for TRIM_HORIZON points to the starting position in the shard (say sequence
     * number 1) and subsequent calls after records have been inserted return an iterator that points to the first
     * record in the shard (say 4), so that calling getRecords with the initially returned iterator results in a
     * TrimmedDataAccessException. We have only observed this behavior with local DynamoDB, but still testing it.
     */
    @Test
    void testTrimHorizonChange() {
        // setup
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);

        // return two different physical iterators for the same logical iterators
        String iterator1 = mockShardIterator(iteratorRequest);
        String iterator2 = mockShardIterator(iteratorRequest);
        when(streams.getShardIterator(eq(iteratorRequest)))
            .thenReturn(new GetShardIteratorResult().withShardIterator(iterator1))
            .thenReturn(new GetShardIteratorResult().withShardIterator(iterator2));

        // fail second time called with first iterator (because TRIM_HORIZON has advanced)
        GetRecordsRequest request1 = new GetRecordsRequest().withShardIterator(iterator1);
        GetRecordsResult result1 = new GetRecordsResult().withRecords(Collections.emptyList());
        when(streams.getRecords(eq(request1)))
            .thenReturn(result1)
            .thenThrow(TrimmedDataAccessException.class);

        // return records when called with second
        GetRecordsRequest request2 = new GetRecordsRequest().withShardIterator(iterator2);
        GetRecordsResult result2 = new GetRecordsResult().withRecords(records);
        when(streams.getRecords(eq(request2)))
            .thenReturn(result2);

        final CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // test
        String it1 = cachingStreams.getShardIterator(iteratorRequest).getShardIterator();
        assertTrue(cachingStreams.getRecords(new GetRecordsRequest().withShardIterator(it1)).getRecords().isEmpty());

        // expect to get records back (in particular no TrimmedDataAccessException)
        String it2 = cachingStreams.getShardIterator(iteratorRequest).getShardIterator();
        assertFalse(cachingStreams.getRecords(new GetRecordsRequest().withShardIterator(it2)).getRecords().isEmpty());
    }

    /**
     * Verifies that asking for records starting at trim horizon always returns records at the actual trim horizon
     * offset (not cached). Why is this important, i.e., why can't we just cache 'TRIM_HORIZON' records?
     * <p>
     * Consider the following scenario:
     * <ol>
     * <li>Client 1 queries for records with TRIM_HORIZON: stream returns records 1-1000</li>
     * <li>We cache entry (TRIM_HORIZON -> [1,...,1000])</li>
     * <li>Client 1 stops retrieving records.</li>
     * <li>Stream moves trim horizon to 2001 (i.e., deletes records up to 2000)</li>
     * <li>Client 2 queries for records with TRIM_HORIZON: we service records 1-1000 from cache.</li>
     * <li>Client 2 immediately queries using next iterator, i.e. AFTER_SEQUENCE_NUMBER(1000): we have no records
     * cached, so we query underlying stream, which throws TrimmedDataAccessException.</li>
     * </ol>
     * </p>
     * The problem with this sequence is not that a TDAE was thrown. The sequence above is possible without caching in
     * the picture, since TRIM_HORIZON may advance between receiving some records and going to fetch the next. The
     * problem is that caching TRIM_HORIZON makes this situation more likely.
     */
    @Test
    @Ignore
    void testTrimHorizonMoves() {
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest trimHorizonRequest = new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);

        String trimHorizonIterator1 = mockShardIterator(trimHorizonRequest);
        String trimHorizonIterator2 = mockShardIterator(trimHorizonRequest);

        when(streams.getShardIterator(eq(trimHorizonRequest)))
            .thenReturn(new GetShardIteratorResult().withShardIterator(trimHorizonIterator1))
            .thenReturn(new GetShardIteratorResult().withShardIterator(trimHorizonIterator2));

        when(streams.getRecords(eq(new GetRecordsRequest().withShardIterator(trimHorizonIterator1))))
            .thenReturn(new GetRecordsResult().withRecords(records.subList(0, 5)));
        when(streams.getRecords(eq(new GetRecordsRequest().withShardIterator(trimHorizonIterator2))))
            .thenReturn(new GetRecordsResult().withRecords(records.subList(5, 10)));

        final CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        assertNull(assertGetRecords(cachingStreams, trimHorizonRequest, null, 0, 5));
        assertNull(assertGetRecords(cachingStreams, trimHorizonRequest, null, 5, 10));
    }

    /**
     * Verifies that concurrent requests detect overlapping records and merge cache records.
     */
    @Test
    void testConcurrentRetrieves() {
        final AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest request = newAtSequenceNumberRequest(0);

        String iterator1 = mockShardIterator(request);
        String iterator2 = mockShardIterator(request);

        when(streams.getShardIterator(eq(request)))
            .thenReturn(new GetShardIteratorResult().withShardIterator(iterator1))
            .thenReturn(new GetShardIteratorResult().withShardIterator(iterator2));

        // pretend more records have come along by the time the second iterator is used to get records
        mockGetRecords(streams, iterator2, 0, 5);

        final CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        doAnswer(invocation -> {
            assertGetRecords(cachingStreams, request, null, 0, 5);
            return new GetRecordsResult().withRecords(records.subList(0, 7));
        }).when(streams).getRecords(new GetRecordsRequest().withShardIterator(iterator1));

        assertGetRecords(cachingStreams, request, null, 0, 7);
        assertGetRecords(cachingStreams, newAtSequenceNumberRequest(0), null, 0, 7);

        // Only the two trim horizon calls should result in iterator and record lookups. The final call should be
        // serviced from the merged cache segment without needing to lookup another iterator.
        assertCacheMisses(streams, 2, 2);
    }
}
