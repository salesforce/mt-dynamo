package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;
import com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams;
import java.util.Objects;

/**
 * A multitenant version of {@link AmazonDynamoDBStreams} that returns only results for the appropriate tenant.
 */
public interface MtAmazonDynamoDbStreams extends AmazonDynamoDBStreams {

    /**
     * Extends {@link GetRecordsResult} to add the last sequence number observed in the underlying multitenant stream,
     * so that clients can resume iteration where they left off if they do not hold on to the shard iterator.
     */
    class MtGetRecordsResult extends GetRecordsResult {

        /**
         * The last sequence number observed in the underlying multitenant stream.
         */
        private String lastSequenceNumber;

        public String getLastSequenceNumber() {
            return lastSequenceNumber;
        }

        public void setLastSequenceNumber(String lastSequenceNumber) {
            this.lastSequenceNumber = lastSequenceNumber;
        }

        public MtGetRecordsResult withLastSequenceNumber(String lastSequenceNumber) {
            setLastSequenceNumber(lastSequenceNumber);
            return this;
        }

        @Override
        public MtGetRecordsResult withRecords(java.util.Collection<Record> records) {
            setRecords(records);
            return this;
        }

        @Override
        public MtGetRecordsResult withNextShardIterator(String nextShardIterator) {
            setNextShardIterator(nextShardIterator);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            final MtGetRecordsResult that = (MtGetRecordsResult) o;
            return Objects.equals(lastSequenceNumber, that.lastSequenceNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), lastSequenceNumber);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            if (getRecords() != null) {
                sb.append("Records: ").append(getRecords()).append(",");
            }
            if (getNextShardIterator() != null) {
                sb.append("NextShardIterator: ").append(getNextShardIterator());
            }
            if (getLastSequenceNumber() != null) {
                sb.append("LastSequenceNumber: ").append(getLastSequenceNumber());
            }
            sb.append("}");
            return sb.toString();
        }


        @Override
        public MtGetRecordsResult clone() {
            return (MtGetRecordsResult) super.clone();
        }

    }

    /**
     * Returns an appropriate {@link MtAmazonDynamoDbStreams} instance for the given {@link AmazonDynamoDB} instance.
     * <p>
     * Note: if you are using a shared table strategy or generally plan to consume table streams with multiple clients
     * (e.g., one per tenant), then it is recommended to wrap the {@link AmazonDynamoDBStreams} instance with a
     * {@link CachingAmazonDynamoDbStreams}.
     * </p>
     *
     * @param dynamoDb        the {@link AmazonDynamoDB} instance being used for streaming
     * @param dynamoDbStreams the underlying {@link AmazonDynamoDBStreams} instance
     * @return the appropriate {@link MtAmazonDynamoDbStreams} instance for the given {@link AmazonDynamoDB}
     */
    static MtAmazonDynamoDbStreams createFromDynamo(AmazonDynamoDB dynamoDb, AmazonDynamoDBStreams dynamoDbStreams) {
        checkArgument(dynamoDb instanceof MtAmazonDynamoDbBase);

        if (dynamoDb instanceof MtAmazonDynamoDbByTable) {
            return new MtAmazonDynamoDbStreamsByTable(dynamoDbStreams, (MtAmazonDynamoDbByTable) dynamoDb);
        }

        if (dynamoDb instanceof MtAmazonDynamoDbBySharedTable) {
            return new MtAmazonDynamoDbStreamsBySharedTable(dynamoDbStreams, (MtAmazonDynamoDbBySharedTable) dynamoDb);
        }

        throw new UnsupportedOperationException(dynamoDb.getClass().getName() + " is currently not supported");
    }

}
