package com.salesforce.dynamodbv2.testsupport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.salesforce.dynamodbv2.mt.mappers.DelegatingAmazonDynamoDbStreams;

/**
 * Counts calls to {@link AmazonDynamoDBStreams#getShardIterator(GetShardIteratorRequest)} and
 * {@link AmazonDynamoDBStreams#getRecords(GetRecordsRequest)}, so we can make assertions
 * about cache hits and misses.
 */
public class CountingAmazonDynamoDbStreams extends DelegatingAmazonDynamoDbStreams {
    public int getRecordsCount;
    public int getShardIteratorCount;

    public CountingAmazonDynamoDbStreams(AmazonDynamoDBStreams delegate) {
        super(delegate);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        getShardIteratorCount++;
        return super.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        getRecordsCount++;
        return super.getRecords(getRecordsRequest);
    }
}
