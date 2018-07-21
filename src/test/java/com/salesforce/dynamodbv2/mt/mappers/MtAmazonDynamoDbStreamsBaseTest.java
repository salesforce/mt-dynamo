package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

abstract class MtAmazonDynamoDbStreamsBaseTest<T extends MtAmazonDynamoDbStreamsBase> {

    private AmazonDynamoDBStreams amazonDynamoDbStreams;

    private T mtDynamoDbStreamsBase;

    @BeforeEach
    final void setUp() {
        amazonDynamoDbStreams = mock(AmazonDynamoDBStreams.class);
        mtDynamoDbStreamsBase = instantiateUnitUnderTest(amazonDynamoDbStreams);
    }

    protected abstract T instantiateUnitUnderTest(AmazonDynamoDBStreams dynamoDbStreams);

    T getMtStreamsInstance() {
        return mtDynamoDbStreamsBase;
    }

    AmazonDynamoDBStreams getMockedDynamoStreams() {
        return amazonDynamoDbStreams;
    }

    @Test
    void testSetEndpoint() {
        assertUnsupported(() -> mtDynamoDbStreamsBase.setEndpoint(""));
    }

    @Test
    void testSetRegion() {
        assertUnsupported(() -> mtDynamoDbStreamsBase.setRegion(null));
    }

    @Test
    void testDescribeStream() {
        // Should just pass through
        DescribeStreamResult expected = mock(DescribeStreamResult.class);
        given(amazonDynamoDbStreams.describeStream(any())).willReturn(expected);

        DescribeStreamRequest request = mock(DescribeStreamRequest.class);
        DescribeStreamResult actual = mtDynamoDbStreamsBase.describeStream(request);

        assertEquals(expected, actual);
        then(amazonDynamoDbStreams).should().describeStream(request);
    }

    @Test
    void shutdown() {
        mtDynamoDbStreamsBase.shutdown();

        then(amazonDynamoDbStreams).should().shutdown();
    }

    @Test
    void getCachedResponseMetadata() {
        assertUnsupported(() -> mtDynamoDbStreamsBase.getCachedResponseMetadata(mock(AmazonWebServiceRequest.class)));
    }

    private void assertUnsupported(Executable executable) {
        assertThrows(UnsupportedOperationException.class, executable);
    }
}