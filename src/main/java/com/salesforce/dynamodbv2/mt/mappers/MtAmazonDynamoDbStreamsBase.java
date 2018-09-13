package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;

public abstract class MtAmazonDynamoDbStreamsBase extends AbstractAmazonDynamoDBStreams
    implements MtAmazonDynamoDbStreams {

    private final AmazonDynamoDBStreams amazonDynamoDbStreams;

    protected MtAmazonDynamoDbStreamsBase(AmazonDynamoDBStreams amazonDynamoDbStreams) {
        this.amazonDynamoDbStreams = amazonDynamoDbStreams;
    }

    @Override
    public final void shutdown() {
        amazonDynamoDbStreams.shutdown();
    }

    @Override
    public final AmazonDynamoDBStreams getAmazonDynamoDbStreams() {
        return amazonDynamoDbStreams;
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
        return amazonDynamoDbStreams.describeStream(request);
    }

    private void deprecated() {
        throw new UnsupportedOperationException("deprecated");
    }
}
