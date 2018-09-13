package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;

/**
 * Convenience base class for streams adapters.
 */
class DelegatingAmazonDynamoDbStreams implements AmazonDynamoDBStreams {

    protected final AmazonDynamoDBStreams delegate;

    DelegatingAmazonDynamoDbStreams(AmazonDynamoDBStreams delegate) {
        this.delegate = delegate;
    }

    @Override
    @Deprecated
    public void setEndpoint(String endpoint) {
        delegate.setEndpoint(endpoint);
    }

    @Override
    @Deprecated
    public void setRegion(Region region) {
        delegate.setRegion(region);
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        return delegate.describeStream(describeStreamRequest);
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        return delegate.getRecords(getRecordsRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return delegate.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        return delegate.listStreams(listStreamsRequest);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return delegate.getCachedResponseMetadata(request);
    }
}
