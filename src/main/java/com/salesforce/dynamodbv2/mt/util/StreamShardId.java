package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Qualifies a shard identifier with its stream arn to form a unique identifier for the shard.
 */
public final class StreamShardId {

    @Nonnull
    private final String streamArn;
    @Nonnull
    private final String shardId;

    /**
     * Creates a new composite identifier from the given stream and shard identifiers.
     *
     * @param streamArn Stream ARN.
     * @param shardId   Shard Id.
     */
    public StreamShardId(String streamArn, String shardId) {
        this.streamArn = checkNotNull(streamArn);
        this.shardId = checkNotNull(shardId);
    }

    /**
     * Returns the Stream ARN part of this identifier.
     *
     * @return Stream ARN.
     */
    public String getStreamArn() {
        return streamArn;
    }

    /**
     * Returns the Shard ID part of this identifier.
     *
     * @return Shard ID.
     */
    public String getShardId() {
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
        final StreamShardId streamShardId = (StreamShardId) o;
        return Objects.equals(streamArn, streamShardId.streamArn)
            && Objects.equals(shardId, streamShardId.shardId);
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
