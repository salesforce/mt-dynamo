package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.google.common.base.MoreObjects;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;
import com.salesforce.dynamodbv2.mt.util.CachingAmazonDynamoDbStreams;
import java.util.Date;
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
         * The sequence number of the first record loaded from the underlying multitenant stream.
         */
        private String firstSequenceNumber;

        /**
         * The approximate creation time of the first record loaded from the underlying multitenant stream.
         */
        private Date firstApproximateCreationDateTime;

        /**
         * The sequence number of the last record loaded from the underlying multitenant stream.
         */
        private String lastSequenceNumber;

        /**
         * The approximate creation time of the last record loaded from the underlying multitenant stream.
         */
        private Date lastApproximateCreationDateTime;

        /**
         * The total number of records loaded from the underlying multitenant stream.
         */
        private Integer recordCount;

        public MtGetRecordsResult() {
            super();
        }

        public String getFirstSequenceNumber() {
            return firstSequenceNumber;
        }

        public void setFirstSequenceNumber(String firstSequenceNumber) {
            this.firstSequenceNumber = firstSequenceNumber;
        }

        public MtGetRecordsResult withFirstSequenceNumber(String firstSequenceNumber) {
            setFirstSequenceNumber(firstSequenceNumber);
            return this;
        }

        public Date getFirstApproximateCreationDateTime() {
            return firstApproximateCreationDateTime;
        }

        public void setFirstApproximateCreationDateTime(Date firstApproximateCreationDateTime) {
            this.firstApproximateCreationDateTime = firstApproximateCreationDateTime;
        }

        public MtGetRecordsResult withFirstApproximateCreationDateTime(Date firstApproximateCreationDateTime) {
            setFirstApproximateCreationDateTime(firstApproximateCreationDateTime);
            return this;
        }

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

        public Date getLastApproximateCreationDateTime() {
            return lastApproximateCreationDateTime;
        }

        public void setLastApproximateCreationDateTime(Date lastApproximateCreationDateTime) {
            this.lastApproximateCreationDateTime = lastApproximateCreationDateTime;
        }

        public MtGetRecordsResult withLastApproximateCreationDateTime(Date lastApproximateCreationDateTime) {
            setLastApproximateCreationDateTime(lastApproximateCreationDateTime);
            return this;
        }

        public Integer getRecordCount() {
            return recordCount;
        }

        public void setRecordCount(Integer recordCount) {
            this.recordCount = recordCount;
        }

        public MtGetRecordsResult withRecordCount(Integer recordCount) {
            setRecordCount(recordCount);
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
            MtGetRecordsResult that = (MtGetRecordsResult) o;
            return Objects.equals(firstSequenceNumber, that.firstSequenceNumber)
                && Objects.equals(firstApproximateCreationDateTime, that.firstApproximateCreationDateTime)
                && Objects.equals(lastSequenceNumber, that.lastSequenceNumber)
                && Objects.equals(lastApproximateCreationDateTime, that.lastApproximateCreationDateTime)
                && Objects.equals(recordCount, that.recordCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), firstSequenceNumber, firstApproximateCreationDateTime,
                lastSequenceNumber, lastApproximateCreationDateTime, recordCount);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("firstSequenceNumber", firstSequenceNumber)
                .add("firstApproximateCreationDateTime", firstApproximateCreationDateTime)
                .add("lastSequenceNumber", lastSequenceNumber)
                .add("lastApproximateCreationDateTime", lastApproximateCreationDateTime)
                .add("recordCount", recordCount)
                .toString();
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
