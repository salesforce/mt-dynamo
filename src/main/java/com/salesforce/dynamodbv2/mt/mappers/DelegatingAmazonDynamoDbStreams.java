package com.salesforce.dynamodbv2.mt.mappers;

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
public class DelegatingAmazonDynamoDbStreams implements AmazonDynamoDBStreams {

    protected final AmazonDynamoDBStreams dynamoDbStreams;

    protected DelegatingAmazonDynamoDbStreams(AmazonDynamoDBStreams dynamoDbStreams) {
        this.dynamoDbStreams = dynamoDbStreams;
    }

    public AmazonDynamoDBStreams getAmazonDynamoDbStreams() {
        return dynamoDbStreams;
    }

    @Override
    @Deprecated
    public void setEndpoint(String endpoint) {
        dynamoDbStreams.setEndpoint(endpoint);
    }

    @Override
    @Deprecated
    public void setRegion(Region region) {
        dynamoDbStreams.setRegion(region);
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        return dynamoDbStreams.describeStream(describeStreamRequest);
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        return dynamoDbStreams.getRecords(getRecordsRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return dynamoDbStreams.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        return dynamoDbStreams.listStreams(listStreamsRequest);
    }

    @Override
    public void shutdown() {
        dynamoDbStreams.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return dynamoDbStreams.getCachedResponseMetadata(request);
    }
}
