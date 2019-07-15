package com.salesforce.dynamodbv2.mt.util;

import static com.salesforce.dynamodbv2.mt.util.StreamShardPosition.at;
import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockRecord;
import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockRecords;
import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockSequenceNumber;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.util.StreamsRecordCache.Segment;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamsRecordCacheTest {

    private static BigInteger sn(int sn) {
        return at(mockSequenceNumber(sn));
    }

    static List<Arguments> subSegmentArgs() {
        return Arrays.asList(
            Arguments.of(
                new Segment(sn(1), sn(3), mockRecords(1, 2)),
                null, null,
                new Segment(sn(1), sn(3), mockRecords(1, 2))
            ),
            Arguments.of(
                new Segment(sn(1), sn(5), mockRecords(2, 3)),
                sn(4), null,
                new Segment(sn(4), sn(5), emptyList())
            ),
            Arguments.of(
                new Segment(sn(2), sn(4), mockRecords(2, 3)),
                null, sn(3),
                new Segment(sn(2), sn(3), mockRecords(2))
            ),
            Arguments.of(
                new Segment(sn(1), sn(5), mockRecords(2, 3)),
                sn(3), sn(4),
                new Segment(sn(3), sn(4), mockRecords(3))
            ),
            Arguments.of(
                new Segment(sn(1), sn(2), mockRecords(1)),
                sn(1), sn(2),
                new Segment(sn(1), sn(2), mockRecords(1))
            )
        );
    }

    /**
     * Verifies that sub-segments are computed correctly.
     */
    @ParameterizedTest
    @MethodSource("subSegmentArgs")
    void testSubSegment(Segment segment, BigInteger from, BigInteger to, Segment expected) {
        assertEquals(expected, segment.subSegment(from, to));
    }

    static List<Arguments> getRecordsArgs() {
        return Arrays.asList(
            Arguments.of(
                new Segment(sn(1), sn(5), mockRecords(1, 2, 3, 4)),
                sn(1),
                mockRecords(1, 2, 3, 4)
            ),
            Arguments.of(
                new Segment(sn(1), sn(5), mockRecords(1, 2, 3, 4)),
                sn(2),
                mockRecords(2, 3, 4)
            ),
            Arguments.of(
                new Segment(sn(1), sn(5), mockRecords(1, 3, 4)),
                sn(2),
                mockRecords(3, 4)
            ),
            Arguments.of(
                new Segment(sn(1), sn(5), emptyList()),
                sn(2),
                emptyList()
            )
        );
    }

    /**
     * Verifies that sub-list of records is computed correctly.
     */
    @ParameterizedTest
    @MethodSource("getRecordsArgs")
    void testGetRecords(Segment segment, BigInteger from, List<Record> expected) {
        assertEquals(expected, segment.getRecords(from));
    }

    /**
     * Verifies that if a new segment overlaps with the previous segment, overlapping leading records are truncated.
     */
    @Test
    void testPutPreviousOverlap() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final List<Record> records = Arrays.asList(
            mockRecord(1),
            mockRecord(3),
            mockRecord(5),
            mockRecord(8),
            mockRecord(11)
        );

        final StreamShardPosition position1 = at("stream1", "shard1", "0");
        final List<Record> records1 = records.subList(0, 3);
        sut.putRecords(position1, records1);
        assertEquals(records1, sut.getRecords(position1, 10));

        final StreamShardPosition position2 = at("stream1", "shard1", "4");
        final List<Record> records2 = records.subList(2, 5);
        sut.putRecords(position2, records2);
        assertEquals(records2, sut.getRecords(position2, 10));

        assertEquals(records, sut.getRecords(position1, 10));
    }

    /**
     * Verifies that if a new segment overlaps with the next segment, overlapping trailing records are truncated.
     */
    @Test
    void testPutNextOverlap() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final List<Record> records = Arrays.asList(
            mockRecord(1),
            mockRecord(3),
            mockRecord(5),
            mockRecord(8),
            mockRecord(11)
        );

        final StreamShardPosition position1 = at("stream1", "shard1", "4");
        final List<Record> records1 = records.subList(2, 5);
        sut.putRecords(position1, records1);
        assertEquals(records1, sut.getRecords(position1, 10));

        final StreamShardPosition position2 = at("stream1", "shard1", "0");
        final List<Record> records2 = records.subList(0, 4);
        sut.putRecords(position2, records2);
        assertEquals(records1, sut.getRecords(position1, 10));

        assertEquals(records, sut.getRecords(position2, 10));
    }

    /**
     * Verifies that putting records into different shards works (provided cache size is not exceeded).
     */
    @Test
    void testPutDifferentShards() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardPosition position1 = at("stream1", "shard1", "0");
        final List<Record> records1 = Arrays.asList(mockRecord(1), mockRecord(3));
        sut.putRecords(position1, records1);

        final StreamShardPosition position2 = at("stream1", "shard2", "10");
        final List<Record> records2 = Arrays.asList(mockRecord(11), mockRecord(13));
        sut.putRecords(position2, records2);

        assertEquals(records1, sut.getRecords(position1, 10));
        assertEquals(records2, sut.getRecords(position2, 10));
    }

    /**
     * Verifies that segments in shard are removed if cache limit is exceeded.
     */
    @Test
    void testEvictionSameShard() {
        final StreamsRecordCache sut = new StreamsRecordCache(3L);

        final StreamShardPosition position1 = at("stream1", "shard1", "5");
        final List<Record> records1 = Arrays.asList(mockRecord(5), mockRecord(8));
        sut.putRecords(position1, records1);
        assertEquals(records1, sut.getRecords(position1, 10));

        // second segment overlaps with first, expect to adjust to fix, then evict first
        final StreamShardPosition position2 = at("stream1", "shard1", "0");
        final List<Record> records2 = Arrays.asList(mockRecord(1), mockRecord(3), mockRecord(5));
        sut.putRecords(position2, records2);

        assertEquals(emptyList(), sut.getRecords(position1, 10));
        assertEquals(records2.subList(0, 2), sut.getRecords(position2, 10));
    }

    /**
     * Verifies that segments in different shards are evicted in order they were written.
     */
    @Test
    void testEvictionDifferentShard() {
        final StreamsRecordCache sut = new StreamsRecordCache(3L);

        final StreamShardPosition position1 = at("stream1", "shard1", "5");
        final List<Record> records1 = Arrays.asList(mockRecord(5), mockRecord(8));
        sut.putRecords(position1, records1);
        assertEquals(records1, sut.getRecords(position1, 10));

        final StreamShardPosition position2 = at("stream1", "shard2", "10");
        final List<Record> records2 = Arrays.asList(mockRecord(11), mockRecord(13));
        sut.putRecords(position2, records2);

        assertEquals(emptyList(), sut.getRecords(position1, 10));
        assertEquals(records2, sut.getRecords(position2, 10));
    }

    /**
     * Verifies that empty list is returned for shards that have no segments yet.
     */
    @Test
    void testGetNoShard() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);
        assertEquals(emptyList(), sut.getRecords(at("stream1", "shard1", "0"), 10));
    }

    /**
     * Verifies that empty list is returned if there is no segment preceding sequence number.
     */
    @Test
    void testGetNoEntry() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");
        final StreamShardPosition position = at(streamShardId, "5");
        final List<Record> records = Arrays.asList(mockRecord(5), mockRecord(8));
        sut.putRecords(position, records);

        assertEquals(emptyList(), sut.getRecords(at(streamShardId, "0"), 10));
    }

    /**
     * Verifies that empty list is returned if preceding segment does not include sequence number.
     */
    @Test
    void testGetNoEntryOverlap() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");
        final StreamShardPosition position = at(streamShardId, "0");
        final List<Record> records = Arrays.asList(mockRecord(1), mockRecord(3));
        sut.putRecords(position, records);

        assertEquals(emptyList(), sut.getRecords(at(streamShardId, "4"), 10));
    }

    /**
     * Verifies that segment is returned if sequence number matches starting point exactly.
     */
    @Test
    void testExactGetHit() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");
        final StreamShardPosition position = at(streamShardId, "0");
        final List<Record> records = Arrays.asList(mockRecord(1), mockRecord(3));
        sut.putRecords(position, records);

        assertEquals(records, sut.getRecords(at(streamShardId, "0"), 5));
    }

    /**
     * Verifies that partial segment is returned if sequence number is included in it.
     */
    @Test
    void testPartialHit() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");
        final StreamShardPosition position = at(streamShardId, "0");
        final List<Record> records = Arrays.asList(mockRecord(1), mockRecord(3));
        sut.putRecords(position, records);

        assertEquals(records.subList(1, 2), sut.getRecords(at(streamShardId, "2"), 2));
    }

    /**
     * Verifies that adjacent segments are returned.
     */
    @Test
    void testGetAdjacent() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");

        final List<Record> records = Arrays.asList(
            mockRecord(1),
            mockRecord(3),
            mockRecord(5),
            mockRecord(8),
            mockRecord(11)
        );

        sut.putRecords(at(streamShardId, "4"), records.subList(2, 4));
        sut.putRecords(at(streamShardId, "0"), records.subList(0, 4));
        sut.putRecords(at(streamShardId, "6"), records.subList(3, 5));

        assertEquals(records, sut.getRecords(at(streamShardId, "1"), 10));
    }

    /**
     * Verifies that subset of segment is returned if limit is reached.
     */
    @Test
    void testGetLimit() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");
        final StreamShardPosition position = at(streamShardId, "0");
        final List<Record> records = Arrays.asList(mockRecord(1), mockRecord(3), mockRecord(5));
        sut.putRecords(position, records);

        assertEquals(records.subList(0, 2), sut.getRecords(at(streamShardId, "0"), 2));
    }

    /**
     * Verifies that adjacent segments are returned, but only up to limit.
     */
    @Test
    void testGetLimitAdjacent() {
        final StreamsRecordCache sut = new StreamsRecordCache(Long.MAX_VALUE);

        final StreamShardId streamShardId = new StreamShardId("stream1", "shard1");

        final List<Record> records = Arrays.asList(
            mockRecord(1),
            mockRecord(3),
            mockRecord(5),
            mockRecord(8),
            mockRecord(11)
        );

        sut.putRecords(at(streamShardId, "0"), records.subList(0, 2));
        sut.putRecords(at(streamShardId, "0"), records.subList(0, 4));
        sut.putRecords(at(streamShardId, "0"), records.subList(0, 5));

        assertEquals(records.subList(0, 3), sut.getRecords(at(streamShardId, "1"), 3));
    }
}
