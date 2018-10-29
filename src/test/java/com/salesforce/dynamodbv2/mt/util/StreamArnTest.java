package com.salesforce.dynamodbv2.mt.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamArnTest {

    private static Stream<Arguments> args() throws UnsupportedEncodingException {
        String qualifier = "aws:dynamodb:us-east-1:123456789012:";
        String tableName = "mt_sharedtablestatic_s_s";
        String streamLabel = "2015-05-11T21:21:33.291";
        String context = "account/tenant1";
        String virtualTableName = "books_table/x";

        final String escapedContext = URLEncoder.encode(context, StandardCharsets.UTF_8.name());
        final String escapedVirtualTableName = URLEncoder.encode(virtualTableName, StandardCharsets.UTF_8.name());

        String expectedString1 =
            "arn:" + qualifier + "table/" + tableName + "/stream/"
                + streamLabel;
        StreamArn expectedObject1 = new StreamArn(qualifier, tableName, streamLabel);

        String expectedString2 = expectedString1 + "/context/" + escapedContext + "/tenantTable/"
            + escapedVirtualTableName;
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

    @Test
    void testUnencodedInputsFromAndToString() throws UnsupportedEncodingException {
        String qualifier = "aws:dynamodb:us-east-1:123456789012:";
        String tableName = "mt_sharedtablestatic_s_s";
        String streamLabel = "2015-05-11T21:21:33.291";
        String slashContainingContext = "account/tenant1";
        String slashContainingVirtualTableName = "books_table/x";

        final String string1 = "arn:" + qualifier + "table/" + tableName + "/stream/" + streamLabel;
        final StreamArn object1 = new StreamArn(qualifier, tableName, streamLabel);

        StreamArn derivedObject = StreamArn.fromString(string1);
        assertEquals(object1, derivedObject);

        String derivedString = object1.toString();
        assertEquals(string1, derivedString);

        final String string2 = string1 + "/context/" + slashContainingContext + "/tenantTable/"
            + slashContainingVirtualTableName;
        final StreamArn object2 = new MtStreamArn(qualifier, tableName, streamLabel, slashContainingContext,
            slashContainingVirtualTableName);
        try {
            StreamArn.fromString(string2);
            fail("Expected IllegalArgumentException not encountered");
        } catch (IllegalArgumentException iae) {
            assertNull(iae.getMessage());
        }

        final String derivedString2 = object2.toString();
        final String expectedString2 = string1 + "/context/"
            + URLEncoder.encode(slashContainingContext, StandardCharsets.UTF_8.name())
            + "/tenantTable/"
            + URLEncoder.encode(slashContainingVirtualTableName, StandardCharsets.UTF_8.name());
        assertEquals(expectedString2, derivedString2);
    }
}
