package com.salesforce.dynamodbv2.mt.util;

import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.util.StreamsRecordCache.Segment;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;


class SegmentTest {

    private static final StreamShardId SSID = new StreamShardId("stream1", "shard1");

    /**
     * Verifies that getting sub-segment for sequence number between last record and end returns non-empty segment with
     * no records.
     */
    @Test
    void testNoRecordsSubSegment() {
        final Segment segment = new Segment(SSID, new BigInteger("1"), new BigInteger("5"),
            mockRecords(2, 3), 3L);
        final Segment predecessor = new Segment(SSID, new BigInteger("1"), new BigInteger("4"),
            mockRecords(2, 3), 1L);
        final Segment actual = segment.subSegment(predecessor, null);
        final Segment expected = new Segment(SSID, new BigInteger("4"), new BigInteger("5"),
            Collections.emptyList(), 3L);
        assertEquals(expected, actual);
    }

    /**
     * Verifies that starting at sequence number between start and first record sequence number returns all records.
     */
    @Test
    void testAllRecordsSubSegment() {
        final List<Record> records = mockRecords(2, 3);
        final Segment segment = new Segment(SSID, new BigInteger("0"), new BigInteger("5"), records, 1L);
        final Segment predecessor = new Segment(SSID, new BigInteger("0"), new BigInteger("1"),
            mockRecords(2, 3), 2L);
        final Segment actual = segment.subSegment(predecessor, null);
        final Segment expected = new Segment(SSID, new BigInteger("1"), new BigInteger("5"), records, 2L);
        assertEquals(expected, actual);
    }

    @Test
    void testSomeRecordsSubSegment() {
        final List<Record> records = mockRecords(2, 4);
        final Segment segment = new Segment(SSID, new BigInteger("0"), new BigInteger("6"), records, 2L);
        final Segment predecessor = new Segment(SSID, new BigInteger("0"), new BigInteger("2"),
            mockRecords(1), 1L);
        final Segment successor = new Segment(SSID, new BigInteger("4"), new BigInteger("6"),
            mockRecords(4), 1L);
        final Segment actual = segment.subSegment(predecessor, successor);
        final Segment expected = new Segment(SSID, new BigInteger("2"), new BigInteger("4"),
            records.subList(0, 1), 1L);
        assertEquals(expected, actual);
    }
}
