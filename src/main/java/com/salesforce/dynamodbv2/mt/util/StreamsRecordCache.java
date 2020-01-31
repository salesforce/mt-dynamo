package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getLast;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

/**
 * A cache for DynamoDB Streams Record. Optimizes for scanning adjacent stream records by splitting shards into segments
 * with relative offsets. Each segment is cached under its starting sequence number and contains its end point as well
 * as the set of records contained in the segment in the underlying shard. A single cache instance can cache records for
 * multiple streams and shards, and eviction is managed by configuring the maximum number of record bytes to cache. Note
 * that this number corresponds to actual byte size in the underlying stream; in-memory size of the objects is likely
 * larger (by a constant factor) due to JVM overhead. Eviction is managed in FIFO order at the granularity of segments,
 * i.e., if the size of the cache is exceeded, the oldest segments are removed. Note that age of segments m
 */
class StreamsRecordCache {

    /**
     * A cached segment of a stream shard. Begins with {@link #start} (inclusive) and ends with {@link #end} exclusive.
     * The {@link #records} collection is sorted by record sequence number and may be empty. All sequence numbers in the
     * collection fall into the segment sequence number range.
     */
    @VisibleForTesting
    static final class Segment implements Comparable<Segment> {

        @Nonnull
        private final StreamShardId streamShardId;
        @Nonnull
        private final BigInteger start;
        @Nonnull
        private final BigInteger end;
        @Nonnull
        private final List<Record> records;
        private final long byteSize;
        private final long creationTime;

        /**
         * Convenience constructor that initializes {@link #end} to the sequence number following that of the last
         * record.
         *
         * @param start   Starting point of this segment.
         * @param records Collection of records contained in this segment. Must not be empty.
         */
        Segment(StreamShardId streamShardId, BigInteger start, List<Record> records) {
            this(streamShardId, start, getLastSequenceNumber(records), records, getApproximateCreationTime(records));
        }

        /**
         * Creates a new cache segment from {@link #start} inclusive to {@link #end} exclusive containing the given set
         * of records.
         *
         * @param start   Starting point of this segment (inclusive).
         * @param end     Ending point of this segment (exclusive).
         * @param records Set of records contained in the stream for the given range.
         */
        Segment(@NotNull StreamShardId streamShardId, BigInteger start, BigInteger end, List<Record> records,
                long creationTime) {
            assert start.compareTo(end) <= 0;
            this.creationTime = creationTime;
            this.streamShardId = streamShardId;
            this.start = checkNotNull(start);
            this.end = checkNotNull(end);
            this.records = copyOf(checkNotNull(records));
            this.byteSize = records.stream().map(Record::getDynamodb).mapToLong(StreamRecord::getSizeBytes).sum();
        }

        @Nonnull
        StreamShardId getStreamShardId() {
            return streamShardId;
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

            if (start.equals(from)) {
                return records;
            }

            return records.subList(getIndex(from), records.size());
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
         * Returns the approximate creation time of records in this segment.
         *
         * @return Approximate creation time.
         */
        long getCreationTime() {
            return creationTime;
        }

        /**
         * Returns a new segment that has no overlap with the given predecessor or successor, i.e., its {@link #start}
         * sequence number is greater or equal to {@link #end} of the predecessor and its {@link #end} is smaller or
         * equal to {@link #start} of the successor. Any records outside of those bounds are discarded. The timestamp
         * of the new segment is guaranteed to be greater or equal to the timestamp of the predecessor and smaller or
         * equal to the timestamp of the successor.
         *
         * @param predecessor Segment immediately preceding this one in the shard.
         * @param successor   Segment immediately succeeding this one in the shard. Must not overlap with predecessor.
         * @return Subsegment  That has no overlap with predecessor or successor.
         */
        Segment subsegment(@Nullable Segment predecessor, @Nullable Segment successor) {
            // if this is the first record
            if (predecessor == null && successor == null) {
                return this;
            }

            // if predecessor range overlaps with this range, adjust start point
            final BigInteger start = predecessor != null && predecessor.getEnd().compareTo(this.start) > 0
                ? predecessor.getEnd() : this.start;
            // if successor range overlaps with this range, adjust end point
            final BigInteger end = successor != null && successor.getStart().compareTo(this.end) < 0
                ? successor.getStart() : this.end;

            // discard records if we have adjusted start or end point
            final List<Record> records = start.equals(this.start)
                ? (end.equals(this.end)
                // neither predecessor nor successor overlaps: keep records
                ? this.records
                // successor overlaps: drop trailing records
                : copyOf(this.records.subList(0, getIndex(end))))
                : (end.equals(this.end)
                // predecessor overlaps: drop leading records
                ? copyOf(this.records.subList(getIndex(start), this.records.size()))
                // both successor and predecessor overlap: drop leading and trailing records
                : copyOf(this.records.subList(getIndex(start), getIndex(end))));

            // make sure creation time smaller than predecessor's and larger than successor's to evict in correct order
            long creationTime = records.isEmpty() ? this.creationTime : getApproximateCreationTime(records);
            if (predecessor != null) {
                creationTime = Math.max(creationTime, predecessor.creationTime);
            }
            if (successor != null) {
                creationTime = Math.min(creationTime, successor.creationTime);
            }

            return new Segment(streamShardId, start, end, records, creationTime);
        }

        /**
         * Internal helper method to efficiently find the index of the given sequence number in the list of records.
         * Computes the index in the list, such that all records in the list after the index have sequence numbers that
         * are greater or equal to the given sequence number. If no such records exist, returns the size of the list.
         *
         * @param sequenceNumber Sequence number to find index of.
         * @return Index in list for given sequence number.
         */
        private int getIndex(BigInteger sequenceNumber) {
            final List<BigInteger> sequenceNumbers = Lists.transform(records, StreamShardPosition::at);
            int index = Collections.binarySearch(sequenceNumbers, sequenceNumber);
            if (index < 0) {
                index = (-index) - 1;
            }
            return index;
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

        /**
         * Establishes order over segments across streams and shards by approximate creation time. The method
         * {@link #subsegment(Segment, Segment)} ensures that {@link #creationTime} of segments for a given shard is
         * monotonically increasing. Tiebreakers are broken by starting sequence number. Since those are guaranteed to
         * be strictly monotonically increasing for a given stream shard, we guarantee that segments within a shard are
         * evicted in order of sequence numbers, i.e., oldest-out-first. Across stream shards, the order is roughly
         * based on the age of segments, though given that we potentially adjust creationTime with adjacent segments,
         * it's not perfect. The idea is similar to hybrid clocks:
         * http://users.ece.utexas.edu/~garg/pdslab/david/hybrid-time-tech-report-01.pdf
         *
         * @param o Other segment to compare to.
         * @return a negative, zero, or positive as first is less than, equal to, or greater than the second.
         */
        @Override
        public int compareTo(@NotNull StreamsRecordCache.Segment o) {
            int c = Long.compare(this.creationTime, o.creationTime);
            if (c != 0) {
                return c;
            }
            c = start.compareTo(o.start);
            if (c != 0) {
                return c;
            }
            c = streamShardId.getShardId().compareTo(o.streamShardId.getShardId());
            if (c != 0) {
                return c;
            }
            return streamShardId.getStreamArn().compareTo(o.streamShardId.getStreamArn());
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
            return creationTime == segment.creationTime
                && streamShardId.equals(segment.streamShardId)
                && start.equals(segment.start)
                && end.equals(segment.end)
                && records.equals(segment.records);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamShardId, start, end, records, creationTime);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("streamShardId", streamShardId)
                .add("start", start)
                .add("end", end)
                .add("records", records)
                .add("byteSize", byteSize)
                .add("creationTime", creationTime)
                .toString();
        }

        /**
         * Returns the approximate creation time of the first record in the collection.
         *
         * @param records Collection of records. Must not be empty.
         * @return Approximate creation time of first record.
         */
        private static long getApproximateCreationTime(List<Record> records) {
            return records.get(0).getDynamodb().getApproximateCreationDateTime().getTime();
        }

        /**
         * Returns the sequence number of the last record in the collection.
         *
         * @param records Collection of records. Must not be empty.
         * @return Sequence number of last record.
         */
        private static BigInteger getLastSequenceNumber(List<Record> records) {
            return StreamShardPosition.after(getLast(records));
        }

    }

    // config parameter
    private final long maxRecordsByteSize;

    // cached record segments sorted by sequence number within each shard
    private final ConcurrentMap<StreamShardId, NavigableMap<BigInteger, Segment>> segments;
    // Eviction order of cache segments
    private final NavigableSet<Segment> evictionQueue;
    // private final Queue<StreamShardPosition> insertionOrder;
    // locks for accessing shard caches
    private final Striped<ReadWriteLock> shardLocks;
    // size of cache in terms of number of records
    private final AtomicLong size;
    // size of cache in terms of number of record bytes
    private final AtomicLong byteSize;
    // meters for observability
    private final Timer getRecordsTime;
    private final DistributionSummary getRecordsHitSize;
    private final Timer putRecordsTime;
    private final DistributionSummary putRecordsSize;
    private final DistributionSummary putRecordsDiscardedSize;
    private final Timer evictRecordsTimer;
    private final DistributionSummary evictRecordsSize;
    private final Timer evictRecordsAge;

    StreamsRecordCache(long maxRecordsByteSize) {
        this(new CompositeMeterRegistry(), maxRecordsByteSize);
    }

    StreamsRecordCache(MeterRegistry meterRegistry, long maxRecordsByteSize) {
        this.maxRecordsByteSize = maxRecordsByteSize;
        this.segments = new ConcurrentHashMap<>();
        this.evictionQueue = new ConcurrentSkipListSet<>();
        this.shardLocks = Striped.lazyWeakReadWriteLock(1024);
        this.size = new AtomicLong(0L);
        this.byteSize = new AtomicLong(0L);

        final String className = StreamsRecordCache.class.getSimpleName();
        this.getRecordsTime = meterRegistry.timer(className + ".GetRecords.Time");
        this.getRecordsHitSize = meterRegistry.summary(className + ".GetRecords.Hit.Size");
        this.putRecordsTime = meterRegistry.timer(className + ".PutRecords.Time");
        this.putRecordsSize = meterRegistry.summary(className + ".PutRecords.Size");
        this.putRecordsDiscardedSize = meterRegistry.summary(className + ".PutRecords.Discarded.Size");
        this.evictRecordsTimer = meterRegistry.timer(className + ".EvictRecords.Time");
        this.evictRecordsSize = meterRegistry.summary(className + ".EvictRecords.Size");
        this.evictRecordsAge = meterRegistry.timer(className + ".EvictRecords.Age");
        meterRegistry.gauge(className + ".size", size);
        meterRegistry.gauge(className + ".byteSize", byteSize);
    }

    /**
     * Returns the set of records cached for the given shard location.
     *
     * @param iteratorPosition Shard location for which to return cached records.
     * @return List of records cached. Empty if none cached
     */
    List<Record> getRecords(StreamShardPosition iteratorPosition, int limit) {
        return getRecordsTime.record(() -> {
            checkArgument(iteratorPosition != null && limit > 0);

            final List<Record> records;
            final StreamShardId streamShardId = iteratorPosition.getStreamShardId();
            final ReadWriteLock lock = shardLocks.get(streamShardId);
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                records = innerGetRecords(streamShardId, iteratorPosition.getSequenceNumber(), limit);
            } finally {
                readLock.unlock();
            }

            // record cache hit, including size
            if (!records.isEmpty()) {
                getRecordsHitSize.record(records.size());
            }

            return records;
        });
    }

    // inner helper method must be called with lock held
    private List<Record> innerGetRecords(StreamShardId streamShardId, BigInteger sequenceNumber, int limit) {
        final NavigableMap<BigInteger, Segment> shardCache = segments.get(streamShardId);
        if (shardCache == null) {
            // nothing cached for the requested shard
            return Collections.emptyList();
        }
        final Entry<BigInteger, Segment> entry = shardCache.floorEntry(sequenceNumber);
        if (entry == null) {
            // no segment with requested or smaller sequence number exists
            return Collections.emptyList();
        }
        final Segment segment = entry.getValue();
        if (segment.getEnd().compareTo(sequenceNumber) <= 0) {
            // preceding segment does not contain requested sequence number (which means there are no records cached yet
            // for the requested sequence number, since otherwise floorEntry would have returned the next higher entry)
            return Collections.emptyList();
        }

        // preceding segment contains (some) records for the requested sequence number
        final List<Record> innerRecords = new ArrayList<>(limit);
        addAll(innerRecords, segment.getRecords(sequenceNumber), limit);

        // keep going through adjacent segments (if present), until limit is reached
        Segment next = segment;
        while (innerRecords.size() < limit) {
            // note: each lookup takes O(log n); could consider linking or merging segments together to avoid this
            next = shardCache.get(next.getEnd());
            if (next == null) {
                break;
            }
            addAll(innerRecords, next.getRecords(), limit);
        }

        return Collections.unmodifiableList(innerRecords);
    }

    // Should we bring back segment merging to avoid cache fragmentation?
    void putRecords(StreamShardPosition iteratorPosition, List<Record> records) {
        putRecordsTime.record(() -> {
            checkArgument(iteratorPosition != null && records != null && !records.isEmpty());

            final StreamShardId streamShardId = iteratorPosition.getStreamShardId();
            final BigInteger sequenceNumber = iteratorPosition.getSequenceNumber();
            final Segment segment = new Segment(streamShardId, sequenceNumber, records);
            final Segment cacheSegment;

            final ReadWriteLock lock = shardLocks.get(streamShardId);
            final Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                final NavigableMap<BigInteger, Segment> shardCache =
                    segments.computeIfAbsent(streamShardId, k -> new TreeMap<>());

                // lookup segments that immediately precede and succeed new segment to drop overlapping records
                cacheSegment = segment.subsegment(
                    getValue(shardCache::floorEntry, sequenceNumber),
                    getValue(shardCache::higherEntry, sequenceNumber)
                );

                // add new segment to the cache, unless it is empty
                if (!cacheSegment.isEmpty()) {
                    shardCache.put(cacheSegment.getStart(), cacheSegment); // log warning if previous element not null?
                    evictionQueue.add(cacheSegment);
                    size.addAndGet(cacheSegment.getRecords().size());
                    byteSize.addAndGet(cacheSegment.getByteSize());
                }
            } finally {
                writeLock.unlock();
            }

            putRecordsSize.record(cacheSegment.getRecords().size());
            putRecordsDiscardedSize.record(segment.getRecords().size() - cacheSegment.getRecords().size());

            // could do asynchronously in the future
            evictRecords();
        });
    }

    /**
     * Evicts records until the cache size is below the max.
     */
    private void evictRecords() {
        evictRecordsTimer.record(() -> {
            int numEvicted = 0;
            while (byteSize.get() > maxRecordsByteSize) {
                final Segment oldest = evictionQueue.pollFirst();
                // note: it's possible that the oldest position is null, since multiple threads may be trying to evict
                // segments concurrently and checking the size and pulling the oldest record are not atomic operations.
                if (oldest != null) {
                    // if we did get a record, we should be able to expect that the shard cache is in a consistent
                    // state, since we lock it for every modification, but null-checks added to be defensive.
                    final StreamShardId streamShardId = oldest.getStreamShardId();
                    final ReadWriteLock lock = shardLocks.get(streamShardId);
                    final Lock writeLock = lock.writeLock();
                    writeLock.lock();
                    try {
                        final NavigableMap<BigInteger, Segment> shard = segments.get(streamShardId);
                        // Could log a warning if there is no shard cache
                        if (shard != null) {
                            final Segment evicted = shard.remove(oldest.getStart());
                            // Could log a warning if there is no segment
                            if (evicted != null) {
                                numEvicted += evicted.getRecords().size();
                                size.addAndGet(-evicted.getRecords().size());
                                byteSize.addAndGet(-evicted.getByteSize());
                                evictRecordsAge.record(System.currentTimeMillis() - evicted.getCreationTime(),
                                    MILLISECONDS);
                                if (shard.isEmpty()) {
                                    segments.remove(streamShardId);
                                }
                            }
                        }
                    } finally {
                        writeLock.unlock();
                    }
                }
            }
            evictRecordsSize.record(numEvicted);
        });
    }

    // helper for getting nullable value from map entry
    private static <K, V> V getValue(Function<K, Entry<K, V>> f, K key) {
        return Optional.ofNullable(f.apply(key)).map(Entry::getValue).orElse(null);
    }

    // helper for adding to list up to specified limit
    private static <T> void addAll(List<T> list, List<T> toAdd, int limit) {
        assert list.size() <= limit;
        final int remaining = limit - list.size();
        if (toAdd.size() <= remaining) {
            list.addAll(toAdd);
        } else {
            list.addAll(toAdd.subList(0, remaining));
        }
    }

}
