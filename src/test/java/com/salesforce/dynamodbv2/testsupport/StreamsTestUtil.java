package com.salesforce.dynamodbv2.testsupport;

import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class StreamsTestUtil {

    /**
     * Creates a mock streams record for unit testing purposes.
     *
     * @param sequenceNumber integer representation of sequence number to assign.
     * @return  Mock Streams record.
     */
    public static Record mockRecord(int sequenceNumber) {
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

    private static String formatSequenceNumber(int sequenceNumber) {
        return String.format("%021d", sequenceNumber);
    }

    public static List<Record> mockRecords(int... sequenceNumbers) {
        return Arrays.stream(sequenceNumbers).mapToObj(StreamsTestUtil::mockRecord).collect(Collectors.toList());
    }

}
