package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import org.junit.jupiter.api.Test;

class MtAmazonDynamoDbStreamsPassthroughTest
        extends MtAmazonDynamoDbStreamsBaseTest<MtAmazonDynamoDbStreamsPassthrough> {
    @Override
    protected MtAmazonDynamoDbStreamsPassthrough instantiateUnitUnderTest(AmazonDynamoDBStreams dynamoDbStreams) {
        return new MtAmazonDynamoDbStreamsPassthrough(dynamoDbStreams);
    }

    @Test
    void getRecords() {
        GetRecordsResult expected = mock(GetRecordsResult.class);
        given(getMockedDynamoStreams().getRecords(any())).willReturn(expected);

        GetRecordsRequest request = mock(GetRecordsRequest.class);
        GetRecordsResult actual = getMtStreamsInstance().getRecords(request);

        assertEquals(expected, actual);
        then(getMockedDynamoStreams()).should().getRecords(request);
    }

    @Test
    void getShardIterator() {
        GetShardIteratorResult expected = mock(GetShardIteratorResult.class);
        given(getMockedDynamoStreams().getShardIterator(any())).willReturn(expected);

        GetShardIteratorRequest request = mock(GetShardIteratorRequest.class);
        GetShardIteratorResult actual = getMtStreamsInstance().getShardIterator(request);

        assertEquals(expected, actual);
        then(getMockedDynamoStreams()).should().getShardIterator(request);
    }

    @Test
    void listStreams() {
        ListStreamsResult expected = mock(ListStreamsResult.class);
        given(getMockedDynamoStreams().listStreams(any())).willReturn(expected);

        ListStreamsRequest request = mock(ListStreamsRequest.class);
        ListStreamsResult actual = getMtStreamsInstance().listStreams(request);

        assertEquals(expected, actual);
        then(getMockedDynamoStreams()).should().listStreams(request);
    }
}