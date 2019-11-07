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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
         * Metrics about the stream segment that was scanned to create the tenant-specific result.
         */
        private StreamSegmentMetrics streamSegmentMetrics;

        public MtGetRecordsResult() {
            super();
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

        public StreamSegmentMetrics getStreamSegmentMetrics() {
            return streamSegmentMetrics;
        }

        public void setStreamSegmentMetrics(StreamSegmentMetrics streamSegmentMetrics) {
            this.streamSegmentMetrics = streamSegmentMetrics;
        }

        public MtGetRecordsResult withStreamSegmentMetrics(StreamSegmentMetrics scannedStreamSegment) {
            setStreamSegmentMetrics(scannedStreamSegment);
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
            return Objects.equals(streamSegmentMetrics, that.streamSegmentMetrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), streamSegmentMetrics);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("streamSegmentMetrics", streamSegmentMetrics)
                .toString();
        }

        @Override
        public MtGetRecordsResult clone() {
            return (MtGetRecordsResult) super.clone();
        }

    }

    /**
     * Metrics about a segment in a record stream shard.
     */
    class StreamSegmentMetrics {

        /**
         * Metrics about the first record in this stream segment.
         */
        private StreamRecordMetrics firstRecordMetrics;

        /**
         * Metrics about the last record in this stream segment.
         */
        private StreamRecordMetrics lastRecordMetrics;

        /**
         * The number of records in this stream segment.
         */
        private Integer recordCount;

        public StreamSegmentMetrics() {
            super();
        }

        public Integer getRecordCount() {
            return recordCount;
        }

        public void setRecordCount(Integer recordCount) {
            this.recordCount = recordCount;
        }

        public StreamSegmentMetrics withRecordCount(Integer recordCount) {
            setRecordCount(recordCount);
            return this;
        }

        public StreamRecordMetrics getFirstRecordMetrics() {
            return firstRecordMetrics;
        }

        public void setFirstRecordMetrics(StreamRecordMetrics firstRecordMetrics) {
            this.firstRecordMetrics = firstRecordMetrics;
        }

        public StreamSegmentMetrics withFirstRecordMetrics(StreamRecordMetrics firstRecordMetrics) {
            setFirstRecordMetrics(firstRecordMetrics);
            return this;
        }

        public StreamRecordMetrics getLastRecordMetrics() {
            return lastRecordMetrics;
        }

        public void setLastRecordMetrics(StreamRecordMetrics lastRecordMetrics) {
            this.lastRecordMetrics = lastRecordMetrics;
        }

        public StreamSegmentMetrics withLastRecordMetrics(StreamRecordMetrics lastRecordMetrics) {
            setLastRecordMetrics(lastRecordMetrics);
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
            StreamSegmentMetrics that = (StreamSegmentMetrics) o;
            return Objects.equals(firstRecordMetrics, that.firstRecordMetrics)
                && Objects.equals(lastRecordMetrics, that.lastRecordMetrics)
                && Objects.equals(recordCount, that.recordCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstRecordMetrics, lastRecordMetrics, recordCount);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("firstRecordMetrics", firstRecordMetrics)
                .add("lastRecordMetrics", lastRecordMetrics)
                .add("recordCount", recordCount)
                .toString();
        }
    }

    /**
     * A projection of {@link com.amazonaws.services.dynamodbv2.model.StreamRecord} that includes only metrics about the
     * record, such as sequence number and creation time, not the actual data.
     */
    class StreamRecordMetrics {
        private String sequenceNumber;
        private Date approximateCreationDateTime;

        public void setSequenceNumber(String sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public String getSequenceNumber() {
            return this.sequenceNumber;
        }

        public StreamRecordMetrics withSequenceNumber(String sequenceNumber) {
            this.setSequenceNumber(sequenceNumber);
            return this;
        }

        public void setApproximateCreationDateTime(Date approximateCreationDateTime) {
            this.approximateCreationDateTime = approximateCreationDateTime;
        }

        public Date getApproximateCreationDateTime() {
            return this.approximateCreationDateTime;
        }

        public StreamRecordMetrics withApproximateCreationDateTime(Date approximateCreationDateTime) {
            this.setApproximateCreationDateTime(approximateCreationDateTime);
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
            StreamRecordMetrics that = (StreamRecordMetrics) o;
            return Objects.equals(sequenceNumber, that.sequenceNumber)
                && Objects.equals(approximateCreationDateTime, that.approximateCreationDateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequenceNumber, approximateCreationDateTime);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("sequenceNumber", sequenceNumber)
                .add("approximateCreationDateTime", approximateCreationDateTime)
                .toString();
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
    static MtAmazonDynamoDbStreamsBase createFromDynamo(AmazonDynamoDB dynamoDb,
                                                        AmazonDynamoDBStreams dynamoDbStreams) {
        checkArgument(dynamoDb instanceof MtAmazonDynamoDbBase);

        if (dynamoDb instanceof MtAmazonDynamoDbByTable) {
            return new MtAmazonDynamoDbStreamsByTable(dynamoDbStreams, (MtAmazonDynamoDbByTable) dynamoDb);
        }

        if (dynamoDb instanceof MtAmazonDynamoDbBySharedTable) {
            return new MtAmazonDynamoDbStreamsBySharedTable(dynamoDbStreams, (MtAmazonDynamoDbBySharedTable) dynamoDb);
        }

        if (dynamoDb instanceof MtAmazonDynamoDbComposite) {
            MtAmazonDynamoDbComposite compositeDb = (MtAmazonDynamoDbComposite) dynamoDb;
            Map<AmazonDynamoDB, MtAmazonDynamoDbStreamsBase> streamsPerDb = compositeDb.getDelegates().stream()
                .collect(Collectors.toMap(db -> db, db -> createFromDynamo(db, dynamoDbStreams)));
            return new MtAmazonDynamoDbStreamsComposite(dynamoDbStreams, compositeDb, streamsPerDb);
        }

        throw new UnsupportedOperationException(dynamoDb.getClass().getName() + " is currently not supported");
    }

}
