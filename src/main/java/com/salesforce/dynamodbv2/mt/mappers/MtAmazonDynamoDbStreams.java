package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.CachingAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;

/**
 * A multi-tenant version of {@link AmazonDynamoDBStreams} that returns only results for the appropriate tenant.
 */
public interface MtAmazonDynamoDbStreams extends AmazonDynamoDBStreams {

    /**
     * Returns an appropriate {@link MtAmazonDynamoDbStreams} instance for the given {@link AmazonDynamoDB} instance.
     *
     * @param dynamoDb        the {@link AmazonDynamoDB} instance being used for streaming
     * @param dynamoDbStreams the underlying {@link AmazonDynamoDBStreams} instance
     * @return the appropriate {@link MtAmazonDynamoDbStreams} instance for the given {@link AmazonDynamoDB}
     */
    static MtAmazonDynamoDbStreams createFromDynamo(AmazonDynamoDB dynamoDb, AmazonDynamoDBStreams dynamoDbStreams) {
        checkArgument(dynamoDb instanceof MtAmazonDynamoDbBase);

        if (dynamoDb instanceof MtAmazonDynamoDbByTable) {
            return new MtAmazonDynamoDbStreamsByTable(dynamoDbStreams, (MtAmazonDynamoDbByTable)dynamoDb);
        }

        if (dynamoDb instanceof MtAmazonDynamoDbBySharedTable) {
            AmazonDynamoDBStreams streams = dynamoDbStreams instanceof CachingAmazonDynamoDbStreams
                    ? dynamoDbStreams
                    : new CachingAmazonDynamoDbStreams.Builder(dynamoDbStreams).build();
            return new MtAmazonDynamoDbStreamsBySharedTable(streams, (MtAmazonDynamoDbBySharedTable)dynamoDb);
        }

        throw new UnsupportedOperationException(dynamoDb.getClass().getName() + " is currently not supported");
    }

    /**
     * Gets the underlying {@link AmazonDynamoDBStreams} instance.
     *
     * @return the underlying {@link AmazonDynamoDBStreams} instance
     */
    AmazonDynamoDBStreams getAmazonDynamoDbStreams();
}
