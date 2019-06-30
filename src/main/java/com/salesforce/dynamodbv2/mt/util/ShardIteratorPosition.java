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
final class ShardIteratorPosition {

    static ShardIteratorPosition at(String streamArn, String shardId, Record record) {
        return at(streamArn, shardId, record.getDynamodb().getSequenceNumber());
    }

    static ShardIteratorPosition at(String streamArn, String shardId, String sequenceNumber) {
        return at(new ShardId(streamArn, shardId), sequenceNumber);
    }

    static ShardIteratorPosition at(ShardId shardId, String sequenceNumber) {
        return new ShardIteratorPosition(shardId, at(sequenceNumber));
    }

    static BigInteger at(Record record) {
        return at(record.getDynamodb().getSequenceNumber());
    }

    static BigInteger at(String sequenceNumber) {
        return new BigInteger(sequenceNumber);
    }

    static ShardIteratorPosition after(String streamArn, String shardId, String sequenceNumber) {
        return after(new ShardId(streamArn, shardId), sequenceNumber);
    }

    static ShardIteratorPosition after(ShardId shardId, String sequenceNumber) {
        return new ShardIteratorPosition(shardId, after(sequenceNumber));
    }

    static BigInteger after(Record record) {
        return after(record.getDynamodb().getSequenceNumber());
    }

    static BigInteger after(String sequenceNumber) {
        return at(sequenceNumber).add(BigInteger.ONE);
    }

    @Nonnull
    private final ShardId shardId;
    @Nonnull
    private final BigInteger sequenceNumber;

    ShardIteratorPosition(ShardId shardId, BigInteger sequenceNumber) {
        this.shardId = checkNotNull(shardId);
        this.sequenceNumber = checkNotNull(sequenceNumber);
    }

    ShardId getShardId() {
        return shardId;
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
        ShardIteratorPosition that = (ShardIteratorPosition) o;
        return Objects.equals(shardId, that.shardId)
            && Objects.equals(sequenceNumber, that.sequenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, sequenceNumber);
    }

}
