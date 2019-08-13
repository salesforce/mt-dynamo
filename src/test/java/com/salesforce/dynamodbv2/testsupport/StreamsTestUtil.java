package com.salesforce.dynamodbv2.testsupport;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.google.common.cache.Cache;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class StreamsTestUtil {

    /**
     * Creates a mock streams record for unit testing purposes.
     *
     * @param sequenceNumber integer representation of sequence number to assign.
     * @return  Mock Streams record.
     */
    public static Record mockRecord(int sequenceNumber) {
        return mockRecord(sequenceNumber, sequenceNumber);
    }

    public static Record mockRecord(int sequenceNumber, long creationTime) {
        return new Record()
            .withEventID(String.valueOf(sequenceNumber))
            .withEventSource("aws:dynamodb")
            .withEventName(OperationType.INSERT)
            .withEventVersion("1.1")
            .withAwsRegion("ddblocal")
            .withDynamodb(new StreamRecord()
                .withSequenceNumber(mockSequenceNumber(sequenceNumber))
                .withSizeBytes(1L)
                .withApproximateCreationDateTime(new Date(creationTime))
            );
    }

    public static String mockSequenceNumber(int sequenceNumber) {
        return String.format("%021d", sequenceNumber);
    }

    public static List<Record> mockRecords(int... sequenceNumbers) {
        return Arrays.stream(sequenceNumbers).mapToObj(StreamsTestUtil::mockRecord).collect(Collectors.toList());
    }

    /**
     * Verifies a cache miss/hit occurs on a describeStreamCache lookup.
     */
    public static DescribeStreamResult verifyDescribeStreamCacheResult(Cache describeStreamCache, String key,
                                                                       boolean expectedCacheHit,
                                                                       DescribeStreamResult expectedResult) {
        DescribeStreamResult cacheLookupResult = (DescribeStreamResult) describeStreamCache.getIfPresent(key);

        if (!expectedCacheHit) {
            assertNull(cacheLookupResult);
        } else {
            assertNotNull(cacheLookupResult);

            // These are the StreamDescription fields we care about validating for the describeStreamCache logic.
            // For example StreamViewType and CreationRequestDateTime are not affected by the cache loader method.
            StreamDescription cacheResultDesc = cacheLookupResult.getStreamDescription();
            StreamDescription expectedResultDesc = expectedResult.getStreamDescription();
            assert (cacheResultDesc.getStreamArn().equals(expectedResultDesc.getStreamArn()));
            assert expectedResult.getStreamDescription().getShards() == null
                || (cacheResultDesc.getShards().equals(expectedResultDesc.getShards()));
            assert cacheResultDesc.getStreamStatus() == null
                || (cacheResultDesc.getStreamStatus().equals(StreamStatus.ENABLED.toString()));
        }

        return cacheLookupResult;
    }

}
