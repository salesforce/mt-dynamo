package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.dynamodbv2.model.Record;
import java.math.BigInteger;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A shard iterator position is a pointer to a sequence number within a stream shard that uniquely identifies the
 * position of a shard iterator. The sequence number may not be used by any record in the stream shard, but rather refer
 * to a logical offset within the shard.
 */
final class StreamShardPosition {

    static StreamShardPosition at(String streamArn, String shardId, Record record) {
        return at(streamArn, shardId, record.getDynamodb().getSequenceNumber());
    }

    static StreamShardPosition at(String streamArn, String shardId, String sequenceNumber) {
        return at(new StreamShardId(streamArn, shardId), sequenceNumber);
    }

    static StreamShardPosition at(StreamShardId streamShardId, String sequenceNumber) {
        return new StreamShardPosition(streamShardId, at(sequenceNumber));
    }

    static BigInteger at(Record record) {
        return at(record.getDynamodb().getSequenceNumber());
    }

    static BigInteger at(String sequenceNumber) {
        return new BigInteger(sequenceNumber);
    }

    static StreamShardPosition after(String streamArn, String shardId, String sequenceNumber) {
        return after(new StreamShardId(streamArn, shardId), sequenceNumber);
    }

    static StreamShardPosition after(StreamShardId streamShardId, String sequenceNumber) {
        return new StreamShardPosition(streamShardId, after(sequenceNumber));
    }

    static BigInteger after(Record record) {
        return after(record.getDynamodb().getSequenceNumber());
    }

    static BigInteger after(String sequenceNumber) {
        return at(sequenceNumber).add(BigInteger.ONE);
    }

    @Nonnull
    private final StreamShardId streamShardId;
    @Nonnull
    private final BigInteger sequenceNumber;

    StreamShardPosition(StreamShardId streamShardId, BigInteger sequenceNumber) {
        this.streamShardId = checkNotNull(streamShardId);
        this.sequenceNumber = checkNotNull(sequenceNumber);
    }

    StreamShardId getStreamShardId() {
        return streamShardId;
    }

    BigInteger getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamShardPosition that = (StreamShardPosition) o;
        return Objects.equals(streamShardId, that.streamShardId)
            && Objects.equals(sequenceNumber, that.sequenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamShardId, sequenceNumber);
    }

}
