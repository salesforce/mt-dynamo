package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

abstract class MTAmazonDynamoDBStreamsBaseTest<T extends MTAmazonDynamoDBStreamsBase> {

    private AmazonDynamoDBStreams amazonDynamoDBStreams;

    private T mtDynamoDBStreamsBase;

    @BeforeEach
    final void setUp() {
        amazonDynamoDBStreams = mock(AmazonDynamoDBStreams.class);
        mtDynamoDBStreamsBase = instantiateUnitUnderTest(amazonDynamoDBStreams);
    }

    protected abstract T instantiateUnitUnderTest(AmazonDynamoDBStreams dynamoDBStreams);

    T getMTStreamsInstance() {
        return mtDynamoDBStreamsBase;
    }

    AmazonDynamoDBStreams getMockedDynamoStreams() {
        return amazonDynamoDBStreams;
    }

    @Test
    void testSetEndpoint() {
        assertUnsupported(() -> mtDynamoDBStreamsBase.setEndpoint(""));
    }

    @Test
    void testSetRegion() {
        assertUnsupported(() -> mtDynamoDBStreamsBase.setRegion(null));
    }

    @Test
    void testDescribeStream() {
        // Should just pass through
        DescribeStreamResult expected = mock(DescribeStreamResult.class);
        given(amazonDynamoDBStreams.describeStream(any())).willReturn(expected);

        DescribeStreamRequest request = mock(DescribeStreamRequest.class);
        DescribeStreamResult actual = mtDynamoDBStreamsBase.describeStream(request);

        assertEquals(expected, actual);
        then(amazonDynamoDBStreams).should().describeStream(request);
    }

    @Test
    void shutdown() {
        mtDynamoDBStreamsBase.shutdown();

        then(amazonDynamoDBStreams).should().shutdown();
    }

    @Test
    void getCachedResponseMetadata() {
        assertUnsupported(() -> mtDynamoDBStreamsBase.getCachedResponseMetadata(mock(AmazonWebServiceRequest.class)));
    }

    private void assertUnsupported(Executable executable) {
        assertThrows(UnsupportedOperationException.class, executable);
    }
}