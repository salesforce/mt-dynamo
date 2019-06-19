package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Qualifies a shard identifier with its stream arn to form a unique identifier for the shard.
 */
final class ShardId {

    @Nonnull
    private final String streamArn;
    @Nonnull
    private final String shardId;

    ShardId(String streamArn, String shardId) {
        this.streamArn = checkNotNull(streamArn);
        this.shardId = checkNotNull(shardId);
    }

    String getStreamArn() {
        return streamArn;
    }

    String getShardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardId shardId = (ShardId) o;
        return Objects.equals(streamArn, shardId.streamArn)
            && Objects.equals(this.shardId, shardId.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamArn, shardId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("streamArn", streamArn)
            .add("shardId", shardId)
            .toString();
    }
}
