package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getLast;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;

/**
 * A cache for DynamoDB Streams Record. Optimizes for scanning adjacent stream records by splitting shards into segments
 * with relative offsets. Each segment is cached under its starting sequence number and contains its end point as well
 * as the set of records contained in the segment in the underlying shard. A single cache instance can cache records for
 * multiple streams and shards, and eviction is managed by configuring the maximum number of record bytes to cache. Note
 * that this number corresponds to actual byte size in the underlying stream; in-memory size of the objects is likely
 * larger (by a constant factor) due to JVM overhead. Eviction is managed in FIFO order at the granularity of segments,
 * i.e., if the size of the cache is exceeded, the oldest segments are removed. Note that age of segments m
 */
class StreamsCache {

    /**
     * A cached segment of a stream shard. Begins with {@link #start} (inclusive) and ends with {@link #end} exclusive.
     * The {@link #records} collection is sorted by record sequence number and may be empty. All sequence numbers in the
     * collection fall into the segment sequence number range.
     */
    @VisibleForTesting
    static final class Segment {

        @Nonnull
        private final BigInteger start;
        @Nonnull
        private final BigInteger end;
        @Nonnull
        private final List<Record> records;
        @Nonnull
        private final long byteSize;

        /**
         * Convenience constructor that initializes {@link #end} to the sequence number following that of the last
         * record.
         *
         * @param start   Starting point of this segment.
         * @param records Collection of records contained in this segment.
         */
        Segment(BigInteger start, List<Record> records) {
            this(start, new BigInteger(getLast(records).getDynamodb().getSequenceNumber()).add(BigInteger.ONE),
                records);
        }

        /**
         * Creates a new cache segment from {@link #start} inclusive to {@link #end} exclusive containing the given set
         * of records.
         *
         * @param start   Starting point of this segment (inclusive).
         * @param end     Ending point of this segment (exclusive).
         * @param records Set of records contained in the stream for the given range.
         */
        Segment(BigInteger start, BigInteger end, List<Record> records) {
            assert start.compareTo(end) <= 0;
            this.start = checkNotNull(start);
            this.end = checkNotNull(end);
            this.records = copyOf(checkNotNull(records));
            this.byteSize = records.stream().map(Record::getDynamodb).mapToLong(StreamRecord::getSizeBytes).sum();
        }

        /**
         * Returns the sequence number at which this segment starts (inclusive).
         *
         * @return Starting point of this segment.
         */
        @Nonnull
        BigInteger getStart() {
            return start;
        }

        /**
         * Returns the sequence number at which this segment ends (exclusive).
         *
         * @return Ending point of this segment.
         */
        @Nonnull
        BigInteger getEnd() {
            return end;
        }

        /**
         * Set of records contained this segment in the underlying stream shard.
         *
         * @return Streams records.
         */
        @Nonnull
        List<Record> getRecords() {
            return records;
        }

        /**
         * Returns the stream records in this segment that have sequence numbers higher than {@param from}.
         *
         * @param from Sequence number from which to retrieve records in this segment. Must greater than or equal to
         *             {@link #start} and less than {@link #end}.
         * @return Set of records in this segment that have sequence numbers higher than {@param from}.
         */
        List<Record> getRecords(BigInteger from) {
            assert start.compareTo(from) <= 0 && end.compareTo(from) > 0;

            if (records.isEmpty()) {
                return records;
            }

            if (start.equals(from)) {
                return records;
            }

            return records.subList(getIndex(from), records.size());
        }

        /**
         * Returns a new segment that starts at the larger of {@link #start} or {@param from} and ends at the smaller of
         * {@link #end} or {@param to}. Both {@param from} and {@param to} may be null. If both are null, this segment
         * is returned. If both are not null, {@param from} must be less than or equal to {@param to}. The set of
         * records in the returned sub-segment are the sub-set of records that fall into the range of the new segment.
         *
         * @param from Starting offset of the new segment, may be null.
         * @param to   Ending offset of the new segment, may be null.
         * @return Sub-segment
         */
        Segment subSegment(BigInteger from, BigInteger to) {
            assert from == null || to == null || from.compareTo(to) <= 0;

            if (from == null && to == null) {
                return this;
            }

            int cf = from == null ? 1 : start.compareTo(from);
            int cl = to == null ? -1 : end.compareTo(to);

            if (cf >= 0) {
                // "start" sequence number of this segment is after "from": start with "start"
                if (cl <= 0) {
                    // "end" sequence number of this segment is before "to": end with "end"
                    return this;
                } else {
                    // "end" sequence number of this segment is after "to": end with "to"
                    final List<Record> newRecords = copyOf(records.subList(0, getIndex(to)));
                    return new Segment(start, to, newRecords);
                }
            } else {
                // "start" sequence number of this segment if before "from": start with "from"
                if (cl <= 0) {
                    // "end" sequence number of this segment is before "to": end with "end"
                    final List<Record> newRecords = copyOf(records.subList(getIndex(from), records.size()));
                    return new Segment(from, end, newRecords);
                } else {
                    // "end" sequence number of this segment is after "to": end with "to"
                    final List<Record> newRecords = copyOf(records.subList(getIndex(from), getIndex(to)));
                    return new Segment(from, to, newRecords);
                }
            }
        }

        /**
         * Internal helper method to efficiently find the index of the given sequence number in the list of records.
         *
         * @param sequenceNumber Sequence number to find index of.
         * @return Index in the list, such that all records in the list after the index have sequence numbers that are
         *     greater or equal to the given sequence number. If no such records exist, the size of the list is
         *     returned.
         */
        private int getIndex(BigInteger sequenceNumber) {
            final List<BigInteger> sequenceNumbers = Lists.transform(records,
                r -> new BigInteger(r.getDynamodb().getSequenceNumber()));
            int index = Collections.binarySearch(sequenceNumbers, sequenceNumber);
            if (index < 0) {
                index = (-index) - 1;
            }
            return index;
        }

        /**
         * Returns the byte size (in the stream) of all records in this segment.
         *
         * @return Byte size of all records in this segment.
         */
        long getByteSize() {
            return byteSize;
        }

        /**
         * Returns whether this segment is empty. Note that non-empty segments may still have an empty records
         * collections if the corresponding segment in the underlying stream contains no records for the sequence number
         * range.
         *
         * @return True if this segment is empty, i.e., if {@link #start} is equal to {@link #end}. False, otherwise.
         */
        boolean isEmpty() {
            return start.equals(end);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Segment segment = (Segment) o;
            return start.equals(segment.start) && end.equals(segment.end) && records.equals(segment.records);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end, records);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("start", start)
                .add("end", end)
                .add("records", records)
                .add("byteSize", byteSize)
                .toString();
        }
    }

    // config parameter
    private final long maxRecordsByteSize;

    // cached record segments sorted by sequence number within each shard
    private final Map<ShardId, NavigableMap<BigInteger, Segment>> segments;
    // Insertion order of cache segments for eviction purposes
    private final LinkedList<ShardLocation> insertionOrder;
    // looks for mutating lock
    private final ReadWriteLock lock;
    // size of cache >= 0
    private long recordsByteSize;

    StreamsCache(long maxRecordsByteSize) {
        this.maxRecordsByteSize = maxRecordsByteSize;
        this.segments = new HashMap<>();
        this.insertionOrder = new LinkedList<>();
        this.lock = new ReentrantReadWriteLock();
        this.recordsByteSize = 0L;
    }

    /**
     * Returns the cached segment that contains the given shard location. If no such segment exists, returns empty.
     *
     * @param shardLocation Shard location for which to return the cached segment that contains it.
     * @return Segment that contains the given location or empty.
     */
    List<Record> getRecords(ShardLocation shardLocation, int limit) {
        assert shardLocation != null && limit > 0;

        final Lock readLock = lock.readLock();
        readLock.lock();
        try {
            final ShardId shardId = shardLocation.getShardId();
            final NavigableMap<BigInteger, Segment> shardCache = segments.get(shardId);
            if (shardCache == null) {
                // nothing cached for the requested shard
                return Collections.emptyList();
            }

            final BigInteger sequenceNumber = shardLocation.getSequenceNumber();
            final Entry<BigInteger, Segment> entry = shardCache.floorEntry(sequenceNumber);
            if (entry == null) {
                // no segment with requested or smaller sequence number
                return Collections.emptyList();
            }

            final Segment segment = entry.getValue();
            if (segment.getEnd().compareTo(sequenceNumber) <= 0) {
                // preceding segment does not include requested sequence number (which means records for the requested
                // sequence number not yet cached, since otherwise floorEntry above would have returned that entry)
                return Collections.emptyList();
            }

            // preceding segment contains (some) records for the requested sequence number
            final List<Record> records = new ArrayList<>(limit);
            addAll(records, segment.getRecords(sequenceNumber), limit);

            // keep going through adjacent segments (if present), until limit is reached
            Segment next = segment;
            while (records.size() < limit) {
                next = shardCache.get(next.getEnd());
                if (next == null) {
                    break;
                }
                addAll(records, next.getRecords(), limit);
            }

            return Collections.unmodifiableList(records);
        } finally {
            readLock.unlock();
        }
    }

    // Should we bring back segment merging to avoid cache fragmentation?
    void putRecords(ShardLocation shardLocation, List<Record> records) {
        assert shardLocation != null && records != null && !records.isEmpty();

        final Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            final ShardId shardId = shardLocation.getShardId();
            final BigInteger sequenceNumber = shardLocation.getSequenceNumber();

            // lookup segments that immediately precede and succeed new segment to drop overlapping records
            Segment segment = new Segment(sequenceNumber, records);

            // adjust segment if there is overlap with existing
            final NavigableMap<BigInteger, Segment> shardCache = segments.get(shardId);
            if (shardCache != null) {
                segment = segment.subSegment(
                    Optional.ofNullable(floorValue(shardCache, sequenceNumber)).map(Segment::getEnd).orElse(null),
                    Optional.ofNullable(higherValue(shardCache, sequenceNumber)).map(Segment::getStart).orElse(null)
                );
            }

            if (!segment.isEmpty()) {
                addSegment(new ShardLocation(shardId, segment.getStart()), segment);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void addSegment(ShardLocation shardLocation, Segment segment) {
        final NavigableMap<BigInteger, Segment> shardCache = segments.computeIfAbsent(shardLocation.getShardId(),
            k -> new TreeMap<>());
        final Segment previous = shardCache.put(shardLocation.getSequenceNumber(), segment);
        assert previous == null;
        insertionOrder.add(shardLocation);
        recordsByteSize += segment.getByteSize();
        while (recordsByteSize > maxRecordsByteSize) {
            removeSegment(insertionOrder.removeFirst());
        }
    }

    private void removeSegment(ShardLocation shardLocation) {
        final NavigableMap<BigInteger, Segment> shardCache = segments.get(shardLocation.getShardId());
        assert !shardCache.isEmpty();
        final Segment segment = shardCache.remove(shardLocation.getSequenceNumber());
        assert segment != null;
        if (shardCache.isEmpty()) {
            segments.remove(shardLocation.getShardId());
        }
        recordsByteSize -= segment.getByteSize();
        assert recordsByteSize >= 0;
    }

    private static <T> void addAll(List<T> list, List<T> toAdd, int limit) {
        assert list.size() <= limit;
        final int remaining = limit - list.size();
        if (toAdd.size() <= remaining) {
            list.addAll(toAdd);
        } else {
            list.addAll(toAdd.subList(0, remaining));
        }
    }

    private static <K, V> V floorValue(NavigableMap<K, V> map, K key) {
        final Entry<K, V> entry = map.floorEntry(key);
        return entry == null ? null : entry.getValue();
    }

    private static <K, V> V higherValue(NavigableMap<K, V> map, K key) {
        final Entry<K, V> entry = map.higherEntry(key);
        return entry == null ? null : entry.getValue();
    }
}
