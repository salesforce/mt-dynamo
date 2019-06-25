package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * A shard location is a pointer to a sequence number within a stream shard that uniquely identifies the position of a
 * record or shard iterator.
 */
final class ShardLocation {

    @Nonnull
    private final ShardId shardId;
    @Nonnull
    private final SequenceNumber sequenceNumber;

    ShardLocation(ShardId shardId, SequenceNumber sequenceNumber) {
        this.shardId = checkNotNull(shardId);
        this.sequenceNumber = checkNotNull(sequenceNumber);
    }

    ShardId getShardId() {
        return shardId;
    }

    SequenceNumber getSequenceNumber() {
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
        ShardLocation that = (ShardLocation) o;
        return Objects.equals(shardId, that.shardId)
            && Objects.equals(sequenceNumber, that.sequenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, sequenceNumber);
    }

}
