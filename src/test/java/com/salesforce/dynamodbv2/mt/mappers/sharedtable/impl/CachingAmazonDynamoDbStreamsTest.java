package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_IMAGE;
import static com.google.common.collect.Iterables.getLast;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.CachingAmazonDynamoDbStreams.Sleeper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

/**
 * Tests the caching streams adapter.
 */
class CachingAmazonDynamoDbStreamsTest {

    /**
     * Counts calls to {@link AmazonDynamoDBStreams#getShardIterator(GetShardIteratorRequest)} and
     * {@link AmazonDynamoDBStreams#getRecords(GetRecordsRequest)}, so we can make assertions
     * about cache hits and misses.
     */
    static class CountingAmazonDynamoDbStreams extends DelegatingAmazonDynamoDbStreams {
        int getRecordsCount;
        int getShardIteratorCount;

        CountingAmazonDynamoDbStreams(AmazonDynamoDBStreams delegate) {
            super(delegate);
        }

        @Override
        public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
            getShardIteratorCount++;
            return super.getShardIterator(getShardIteratorRequest);
        }

        @Override
        public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
            getRecordsCount++;
            return super.getRecords(getRecordsRequest);
        }
    }

    private static final int GET_RECORDS_LIMIT = 1000;
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
     * Test runs against actual DynamoDB (local, but should support remote as well), simulates multiple clients
     * reading the stream at different offsets, and verifies that cache is used to service those requests as
     * expected. The finer-grained verification of various binning and race conditions are tested separately
     * against mock streams, since that's easier to control.
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
            // results should still have been serviced from cache
            assertEquals(1, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

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
            // the records count should now be 2, since we fetched the second page
            assertEquals(2, countingDynamoDbStreams.getRecordsCount);
            // the shard iterator count should still be 1, since we cached 'next iterator' of first page
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

            // second client retrieves remaining chunk of second page
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(nextShardIteratorClient2)
                    .withLimit(limit));
            nextShardIteratorClient2 = result.getNextShardIterator();
            assertNotNull(nextShardIteratorClient2);
            records = result.getRecords();
            assertEquals(GET_RECORDS_LIMIT - limit, records.size());
            // the records count should now be 2, since we fetched the second page
            assertEquals(2, countingDynamoDbStreams.getRecordsCount);
            // the shard iterator count should still be 1, since we cached 'next iterator' of first page
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

            // first client now fetches second page (without limit)
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(nextShardIterator));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(GET_RECORDS_LIMIT, records.size());
            assertEquals(2, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

            // first client now tries to go beyond second page which has no records yet
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(nextShardIterator));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(0, records.size());
            assertEquals(3, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);

            // second client now tries to go beyond second page which still has no records
            result = cachingDynamoDbStreams.getRecords(new GetRecordsRequest()
                    .withShardIterator(nextShardIteratorClient2));
            nextShardIterator = result.getNextShardIterator();
            assertNotNull(nextShardIterator);
            records = result.getRecords();
            assertEquals(0, records.size());
            assertEquals(4, countingDynamoDbStreams.getRecordsCount);
            assertEquals(1, countingDynamoDbStreams.getShardIteratorCount);
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
                        .withSequenceNumber(formatSequenceNumber(sequenceNumber)));
    }

    private static int getMockRecordSequenceNumber(int idx) {
        return Integer.parseInt(records.get(idx).getDynamodb().getSequenceNumber());
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

    private static GetShardIteratorRequest newAfterSequenceNumberRequest(int sequenceNumber) {
        return new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .withSequenceNumber(formatSequenceNumber(sequenceNumber));
    }

    private static void mockGetShardIterator(AmazonDynamoDBStreams streams,
                                             GetShardIteratorRequest iteratorRequest,
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

    private static String mockGetRecords(AmazonDynamoDBStreams streams, String iterator, int from, int to,
                                         Integer next) {
        List<Record> mockRecords = records.subList(from, to);
        String nextIterator = next == null ? null : mockShardIterator(newAfterSequenceNumberRequest(next));
        mockGetRecords(streams, iterator, mockRecords, nextIterator);
        return nextIterator;
    }

    private static String mockGetRecords(AmazonDynamoDBStreams streams, String iterator, int from, int to) {
        return mockGetRecords(streams, iterator, from, to, getMockRecordSequenceNumber(to - 1));
    }

    private static void assertGetRecords(AmazonDynamoDBStreams streams,
                                         String iterator,
                                         Integer limit,
                                         List<Record> records,
                                         String nextIterator) {
        GetRecordsRequest request = new GetRecordsRequest()
                .withShardIterator(iterator)
                .withLimit(limit);
        GetRecordsResult expectedResult = new GetRecordsResult()
                .withRecords(records)
                .withNextShardIterator(nextIterator);
        GetRecordsResult result = streams.getRecords(request);
        assertEquals(expectedResult, result);
    }

    private static void assertGetRecords(AmazonDynamoDBStreams streams,
                                         GetShardIteratorRequest iteratorRequest,
                                         Integer limit,
                                         int from,
                                         int to,
                                         Integer next) {
        final String iterator = streams.getShardIterator(iteratorRequest).getShardIterator();
        assertNotNull(iterator);
        final String nextIterator;
        if (next == null) {
            nextIterator = null;
        } else {
            nextIterator = streams.getShardIterator(new GetShardIteratorRequest()
                    .withStreamArn(iteratorRequest.getStreamArn())
                    .withShardId(iteratorRequest.getShardId())
                    .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .withSequenceNumber(formatSequenceNumber(next)))
                    .getShardIterator();
            assertNotNull(nextIterator);
        }
        final List<Record> mockRecords = records.subList(from, to);
        assertGetRecords(streams, iterator, limit, mockRecords, nextIterator);
    }

    private static void assertGetRecords(AmazonDynamoDBStreams streams,
                                         GetShardIteratorRequest iteratorRequest,
                                         Integer limit,
                                         int from,
                                         int to) {
        assertGetRecords(streams, iteratorRequest, limit, from, to, getMockRecordSequenceNumber(to - 1));
    }

    private static GetShardIteratorRequest mockTrimHorizonRequest(AmazonDynamoDBStreams streams,
                                                                  String streamArn,
                                                                  String shardId) {
        GetShardIteratorRequest thRequest = new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        String iterator = mockGetShardIterator(streams, thRequest);
        mockGetRecords(streams, iterator, 0, 10);
        return thRequest;
    }

    private static CachingAmazonDynamoDbStreams mockTrimHorizonStream(AmazonDynamoDBStreams streams) {
        GetShardIteratorRequest iteratorRequest = mockTrimHorizonRequest(streams, streamArn, shardId);

        // get exact overlapping records with and without limit
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        assertGetRecords(cachingStreams, iteratorRequest, null, 0, 10);

        verify(streams, times(1)).getShardIterator(any());
        verify(streams, times(1)).getRecords(any());

        return cachingStreams;
    }

    private static void assertCacheMisses(AmazonDynamoDBStreams streams, int numGetShardIterators, int numGetRecords) {
        verify(streams, times(numGetShardIterators)).getShardIterator(any());
        verify(streams, times(numGetRecords)).getRecords(any());
    }

    /**
     * Verifies that {@link ShardIteratorType#LATEST} shard iterator type is not supported.
     */
    @Test
    void testLatestNotSupported() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                cachingStreams.getShardIterator(new GetShardIteratorRequest()
                        .withStreamArn(streamArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.LATEST)));
    }

    /**
     * Verifies that {@link ShardIteratorType#AT_SEQUENCE_NUMBER} shard iterator type is not supported.
     */
    @Test
    void testAtSequenceNumberNotSupported() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                cachingStreams.getShardIterator(new GetShardIteratorRequest()
                        .withStreamArn(streamArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)));
    }

    /**
     * Verifies that an exact cache hit can be serviced completely from the cache.
     */
    @Test
    void testExactCacheHit() {
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);
        CachingAmazonDynamoDbStreams cachingStreams = mockTrimHorizonStream(streams);

        assertGetRecords(cachingStreams, newTrimHorizonRequest(), null, 0, 10);

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

        assertGetRecords(cachingStreams, newTrimHorizonRequest(), 5, 0, 5);

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

        assertGetRecords(cachingStreams, newAfterSequenceNumberRequest(40), 10, 5, 10);

        // verify that underlying stream was still accessed only once
        assertCacheMisses(streams, 1, 1);
    }

    /**
     * Verifies that if a new cache segment is added that overlaps with the next segment,
     * the segment is aligned such that overlapping records are not cached twice.
     */
    @Test
    void testOverlappingUpdate() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest afterSnIteratorRequest = newAfterSequenceNumberRequest(40);
        String afterSnIterator = mockGetShardIterator(streams, afterSnIteratorRequest);
        mockGetRecords(streams, afterSnIterator, 5, 10, null);

        GetShardIteratorRequest trimHorizonIteratorRequest = newTrimHorizonRequest();
        String trimHorizonIterator = mockGetShardIterator(streams, trimHorizonIteratorRequest);
        mockGetRecords(streams, trimHorizonIterator, 0, 7);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // test

        // first get records after sequence number, then get records from trim horizon (before)
        assertGetRecords(cachingStreams, afterSnIteratorRequest, null, 5, 10, null);
        assertGetRecords(cachingStreams, trimHorizonIteratorRequest, null, 0, 5);
        assertCacheMisses(streams, 2, 2);

        // now page through and make sure requests serviced from cache
        assertGetRecords(cachingStreams, trimHorizonIteratorRequest, null, 0, 5);
        assertGetRecords(cachingStreams, afterSnIteratorRequest, null, 5, 10, null);
        assertCacheMisses(streams, 2, 2);
    }

    /**
     * Verifies that if a new cache segment is added that overlaps completely with the next,
     * i.e., does not contain any records that are not already in the next, but uses a different
     * shard iterator, the next segment is merged with the new one, meaning all its records are
     * cached using the new shard iterator key and are still accessible via both iterators.
     */
    @Test
    void testCompleteOverlap() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest afterSnIteratorRequest = newAfterSequenceNumberRequest(55); // start between records
        String afterSnIterator = mockGetShardIterator(streams, afterSnIteratorRequest);
        mockGetRecords(streams, afterSnIterator, 6, 9);

        GetShardIteratorRequest afterRecordIteratorRequest = newAfterSequenceNumberRequest(50);
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
     * Verify that we correctly handle an empty page at the end of a stream.
     */
    @Test
    void testEmptyTerminate() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thIteratorRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thIteratorRequest);
        String nextIterator = mockGetRecords(streams, thIterator, 0, 10);

        GetShardIteratorRequest nextIteratorRequest = newAfterSequenceNumberRequest(90);
        mockGetShardIterator(streams, nextIteratorRequest, nextIterator);
        mockGetRecords(streams, nextIterator, Collections.emptyList(), null);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams).build();

        // test
        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 10);
        assertGetRecords(cachingStreams, nextIteratorRequest, null, 0, 0, null);

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

        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 10, null);
        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 10, null);

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
                .withMaxRecordsCacheSize(100)
                .withMaxIteratorCacheSize(100)
                .build();

        final String iterator = cachingStreams.getShardIterator(thIteratorRequest).getShardIterator();
        try {
            cachingStreams.getRecords(new GetRecordsRequest().withShardIterator(iterator));
            fail("Expected limit exceeded exception");
        } catch (LimitExceededException e) {
            // expected
        }

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
        String expiredIterator = mockShardIterator(iteratorRequest);
        String newIterator = mockShardIterator(iteratorRequest);
        when(streams.getShardIterator(iteratorRequest)).thenReturn(
                new GetShardIteratorResult().withShardIterator(expiredIterator),
                new GetShardIteratorResult().withShardIterator(newIterator));

        // calling with expired iterator fails
        when(streams.getRecords(eq(new GetRecordsRequest().withShardIterator(expiredIterator))))
                .thenThrow(new ExpiredIteratorException(""));
        // calling with new iterator passes
        mockGetRecords(streams, newIterator, 0, 10);

        Sleeper sleeper = mock(Sleeper.class);
        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
                .withMaxGetRecordsRetries(1)
                .withSleeper(sleeper)
                .build();

        assertGetRecords(cachingStreams, iteratorRequest, 5, 0, 5);

        assertCacheMisses(streams, 2, 2);
    }

    /**
     * Verifies that records and iterators are evicted from caches per specified max sizes.
     */
    @Test
    void testCacheEviction() {
        // setup
        AmazonDynamoDBStreams streams = mock(AmazonDynamoDBStreams.class);

        GetShardIteratorRequest thIteratorRequest = newTrimHorizonRequest();
        String thIterator = mockGetShardIterator(streams, thIteratorRequest);
        mockGetRecords(streams, thIterator, 0, 5);

        GetShardIteratorRequest asnIteratorRequest = newAfterSequenceNumberRequest(40);
        String asnIterator = mockGetShardIterator(streams, asnIteratorRequest);
        mockGetRecords(streams, asnIterator, 5, 10);

        CachingAmazonDynamoDbStreams cachingStreams = new CachingAmazonDynamoDbStreams.Builder(streams)
                .withMaxIteratorCacheSize(0)
                .withMaxRecordsCacheSize(1)
                .build();

        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 5);
        // nothing cached, so both should be misses
        assertCacheMisses(streams, 1, 1);

        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 5);
        // records are now cached, so no need to load records or iterator
        assertCacheMisses(streams, 1, 1);

        assertGetRecords(cachingStreams, asnIteratorRequest, null, 5, 10);
        // new segment not cached, so need to load records and iterator
        assertCacheMisses(streams, 2, 2);

        assertGetRecords(cachingStreams, asnIteratorRequest, null, 5, 10);
        // new segment still cached, so no need to load records and iterator
        assertCacheMisses(streams, 2, 2);

        assertGetRecords(cachingStreams, thIteratorRequest, null, 0, 5);
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

}
