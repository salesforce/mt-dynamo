package com.salesforce.dynamodbv2.mt.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamArnTest {

    private static Stream<Arguments> args() {
        String qualifier = "aws:dynamodb:us-east-1:123456789012:";
        String tableName = "mt_sharedtablestatic_s_s";
        String streamLabel = "2015-05-11T21:21:33.291";
        String context = "tenant1";
        String virtualTableName = "books_table";

        String expectedString1 =
            "arn:" + qualifier + "table/" + tableName + "/stream/"
                + streamLabel;
        StreamArn expectedObject1 = new StreamArn(qualifier, tableName, streamLabel);

        String expectedString2 = expectedString1 + "/context/" + context + "/tenantTable/"
            + virtualTableName;
        StreamArn expectedObject2 = new MtStreamArn(qualifier, tableName, streamLabel, context, virtualTableName);

        return Stream
            .of(Arguments.of(expectedString1, expectedObject1), Arguments.of(expectedString2, expectedObject2));
    }

    @ParameterizedTest
    @MethodSource("args")
    void testFromString(String expectedString, StreamArn expectedObject) {
        StreamArn actualObject = StreamArn.fromString(expectedString);
        assertEquals(expectedObject, actualObject);
    }

    @ParameterizedTest
    @MethodSource("args")
    void testToString(String expectedString, StreamArn expectedObject) {
        String actualString = expectedObject.toString();
        assertEquals(expectedString, actualString);
    }

}
