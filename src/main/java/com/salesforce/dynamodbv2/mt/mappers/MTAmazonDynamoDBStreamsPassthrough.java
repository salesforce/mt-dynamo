package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;

/**
 * An implementation of {@link MTAmazonDynamoDBStreams} that simply delegates supported calls to the underlying
 * {@link AmazonDynamoDBStreams} instance.
 */
public class MTAmazonDynamoDBStreamsPassthrough extends MTAmazonDynamoDBStreamsBase {

    MTAmazonDynamoDBStreamsPassthrough(AmazonDynamoDBStreams dynamoDBStreams) {
        super(dynamoDBStreams);
    }

    @Override
    public final GetRecordsResult getRecords(GetRecordsRequest request) {
        return getAmazonDynamoDBStreams().getRecords(request);
    }

    @Override
    public final ListStreamsResult listStreams(ListStreamsRequest request) {
        return getAmazonDynamoDBStreams().listStreams(request);
    }

    @Override
    public final GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return getAmazonDynamoDBStreams().getShardIterator(request);
    }
}
