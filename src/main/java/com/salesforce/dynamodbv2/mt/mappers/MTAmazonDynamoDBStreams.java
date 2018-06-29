package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import org.apache.commons.lang3.NotImplementedException;

/**
 * A multi-tenant version of {@link AmazonDynamoDBStreams} that returns only results for the appropriate tenant.
 */
public interface MTAmazonDynamoDBStreams extends AmazonDynamoDBStreams {

    /**
     * Returns an appropriate {@link MTAmazonDynamoDBStreams} instance for the given {@link AmazonDynamoDB} instance.
     *
     * @param dynamoDB        the {@link AmazonDynamoDB} instance being used for streaming
     * @param dynamoDBStreams the underlying {@link AmazonDynamoDBStreams} instance
     * @return the appropriate {@link MTAmazonDynamoDBStreams} instance for the given {@link AmazonDynamoDB}
     */
    static MTAmazonDynamoDBStreams createFromDynamo(AmazonDynamoDB dynamoDB, AmazonDynamoDBStreams dynamoDBStreams) {
        if (dynamoDB instanceof MTAmazonDynamoDBByTable) {
            // By table means streams on a table will be tenant-specific, so just provide a passthrough client
            return new MTAmazonDynamoDBStreamsPassthrough(dynamoDBStreams);
        }

        if (dynamoDB instanceof MTAmazonDynamoDBBase) {
            throw new NotImplementedException(dynamoDB.getClass().getName() + " is not supported");
        }

        return new MTAmazonDynamoDBStreamsPassthrough(dynamoDBStreams);
    }

    /**
     * Gets the underlying {@link AmazonDynamoDBStreams} instance
     *
     * @return the underlying {@link AmazonDynamoDBStreams} instance
     */
    AmazonDynamoDBStreams getAmazonDynamoDBStreams();
}
