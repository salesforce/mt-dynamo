package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;

/**
 * An implementation of {@link MtAmazonDynamoDbStreams} that simply delegates supported calls to the underlying
 * {@link AmazonDynamoDBStreams} instance.
 */
public class MtAmazonDynamoDbStreamsPassthrough extends MtAmazonDynamoDbStreamsBase {

    MtAmazonDynamoDbStreamsPassthrough(AmazonDynamoDBStreams dynamoDbStreams) {
        super(dynamoDbStreams);
    }

    @Override
    public final GetRecordsResult getRecords(GetRecordsRequest request) {
        return getAmazonDynamoDbStreams().getRecords(request);
    }

    @Override
    public final ListStreamsResult listStreams(ListStreamsRequest request) {
        return getAmazonDynamoDbStreams().listStreams(request);
    }

    @Override
    public final GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return getAmazonDynamoDbStreams().getShardIterator(request);
    }
}
