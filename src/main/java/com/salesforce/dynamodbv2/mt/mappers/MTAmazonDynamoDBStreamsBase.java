package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;

public abstract class MTAmazonDynamoDBStreamsBase extends AbstractAmazonDynamoDBStreams
    implements MTAmazonDynamoDBStreams {

    private final AmazonDynamoDBStreams amazonDynamoDBStreams;

    MTAmazonDynamoDBStreamsBase(AmazonDynamoDBStreams amazonDynamoDBStreams) {
        this.amazonDynamoDBStreams = amazonDynamoDBStreams;
    }

    @Override
    public final void shutdown() {
        amazonDynamoDBStreams.shutdown();
    }

    @Override
    public final AmazonDynamoDBStreams getAmazonDynamoDBStreams() {
        return amazonDynamoDBStreams;
    }

    @Override
    public final void setEndpoint(String endpoint) {
        deprecated();
    }

    @Override
    public final void setRegion(Region region) {
        deprecated();
    }

    @Override
    public final DescribeStreamResult describeStream(DescribeStreamRequest request) {
        return amazonDynamoDBStreams.describeStream(request);
    }

    private void deprecated() {
        throw new UnsupportedOperationException("deprecated");
    }
}
