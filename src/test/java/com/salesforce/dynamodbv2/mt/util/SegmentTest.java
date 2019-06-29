package com.salesforce.dynamodbv2.mt.util;

import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.util.StreamsCache.Segment;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;


class SegmentTest {

    /**
     * Verifies that getting sub-segment for sequence number between last record and end returns non-empty segment with
     * no records.
     */
    @Test
    void testNoRecordsSubSegment() {
        final Segment segment = new Segment(new BigInteger("1"), new BigInteger("5"),
            Arrays.asList(mockRecord(2), mockRecord(3)));
        final Segment actual = segment.subSegment(new BigInteger("4"), null);
        final Segment expected = new Segment(new BigInteger("4"), new BigInteger("5"), Collections.emptyList());
        assertEquals(expected, actual);
    }

    /**
     * Verifies that starting at sequence number between start and first record sequence number returns all records.
     */
    @Test
    void testAllRecordsSubSegment() {
        final List<Record> records = Arrays.asList(mockRecord(2), mockRecord(3));
        final Segment segment = new Segment(new BigInteger("0"), new BigInteger("5"), records);
        final Segment actual = segment.subSegment(new BigInteger("1"), null);
        final Segment expected = new Segment(new BigInteger("1"), new BigInteger("5"), records);
        assertEquals(expected, actual);
    }

    @Test
    void testSomeRecordsSubSegment() {
        final List<Record> records = Arrays.asList(mockRecord(2), mockRecord(4));
        final Segment segment = new Segment(new BigInteger("0"), new BigInteger("6"), records);
        final Segment actual = segment.subSegment(new BigInteger("2"), new BigInteger("4"));
        final Segment expected = new Segment(new BigInteger("2"), new BigInteger("4"), records.subList(0, 1));
        assertEquals(expected, actual);
    }
}
