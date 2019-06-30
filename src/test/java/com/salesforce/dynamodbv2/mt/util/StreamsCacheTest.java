package com.salesforce.dynamodbv2.mt.util;

import static com.salesforce.dynamodbv2.testsupport.StreamsTestUtil.mockRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.util.StreamsCache.Segment;
import com.salesforce.dynamodbv2.testsupport.StreamsTestUtil;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StreamsCacheTest {

    static List<Arguments> subSegmentArgs() {
        final List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(new Segment(new BigInteger("1"), new BigInteger("5"), mockRecords(2, 3)),
            new BigInteger("4"), null)
        );

        return args;
    }

    @ParameterizedTest
    @MethodSource("subSegmentArgs")
    void testSubSegment(Segment segment, BigInteger from, BigInteger to, Segment expected) {
        final Segment actual = segment.subSegment(from, to);
        assertEquals(expected, actual);
    }

    @Test
    void testPutPreviousOverlap() {
        final StreamsCache sut = new StreamsCache(Long.MAX_VALUE);

        final List<Record> records = Arrays.asList(
            StreamsTestUtil.mockRecord(1),
            StreamsTestUtil.mockRecord(3),
            StreamsTestUtil.mockRecord(5),
            StreamsTestUtil.mockRecord(8),
            StreamsTestUtil.mockRecord(11)
        );

        final ShardIteratorPosition location1 = ShardIteratorPosition.at("stream1", "shard1", "0");
        final List<Record> records1 = records.subList(0, 3);
        sut.putRecords(location1, records1);

        assertEquals(records1, sut.getRecords(location1, 10));

        final ShardIteratorPosition location2 = ShardIteratorPosition.at("stream1", "shard1", "4");
        final List<Record> records2 = records.subList(2, 5);
        sut.putRecords(location2, records2);

        assertEquals(records, sut.getRecords(location1, 10));
    }


}
