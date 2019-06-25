package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getLast;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;

class StreamsCache {

    /**
     * A cached segment of a stream shard. Begins with {@link #start} (inclusive) and ends with {@link #end} exclusive.
     * The {@link #records} collection is sorted by record sequence number and may be empty. All sequence numbers in the
     * collection fall into the segment sequence number range.
     */
    @VisibleForTesting
    static final class Segment {

        @Nonnull
        private final SequenceNumber start;
        @Nonnull
        private final SequenceNumber end;
        @Nonnull
        private final List<Record> records;
        @Nonnull
        private final long byteSize;

        Segment(SequenceNumber start, List<Record> records) {
            this(start, SequenceNumber.fromRecord(getLast(records)).next(), records);
        }

        Segment(SequenceNumber start, SequenceNumber end, List<Record> records) {
            assert start.compareTo(end) <= 0;
            this.start = checkNotNull(start);
            this.end = checkNotNull(end);
            this.records = copyOf(checkNotNull(records));
            this.byteSize = records.stream().map(Record::getDynamodb).mapToLong(StreamRecord::getSizeBytes).sum();
        }

        @Nonnull
        public SequenceNumber getStart() {
            return start;
        }

        @Nonnull
        public SequenceNumber getEnd() {
            return end;
        }

        @Nonnull
        public List<Record> getRecords() {
            return records;
        }

        List<Record> getRecords(SequenceNumber from) {
            assert start.compareTo(from) <= 0 && end.compareTo(from) >= 0;

            if (records.isEmpty()) {
                return records;
            }

            if (start.equals(from)) {
                return records;
            }

            return records.subList(getIndex(from), records.size());
        }

        private int getIndex(SequenceNumber sequenceNumber) {
            final List<SequenceNumber> sequenceNumbers = Lists.transform(records, SequenceNumber::fromRecord);
            int index = Collections.binarySearch(sequenceNumbers, sequenceNumber);
            if (index < 0) {
                index = (-index) - 1;
            }
            return index;
        }

        Segment subSegment(Segment previousSegment, Segment nextSegment) {
            return subSegment(
                previousSegment == null ? null : previousSegment.getEnd(),
                nextSegment == null ? null : nextSegment.getStart()
            );
        }

        /**
         * Returns a new segment that starts at the larger of {@link #start} or {@param from} and ends at the smaller of
         * {@link #end} or {@param to}. Both {@param from} and {@param to} may be null. If both are null, this segment
         * is returned. If both are not null, {@param from} must be less than or equal to {@param to}. The set of
         * records in the returned sub-segment are the sub-set of records that fall into the range of the new segment.
         *
         * @param from  Starting offset of the new segment, may be null.
         * @param to Ending offset of the new segment, may be null.
         * @return Sub-segment
         */
        Segment subSegment(SequenceNumber from, SequenceNumber to) {
            assert from == null || to == null || from.compareTo(to) <= 0;

            int cf = from == null ? 1 : start.compareTo(from);
            int cl = to == null ? -1 : end.compareTo(to);

            if (cf >= 0) {
                // first sequence number of this segment is after from: start with first
                if (cl <= 0) {
                    // last sequence number of this segment is before to: end with last
                    return this;
                } else {
                    // last sequence number of this segment is after to: end with to
                    final List<Record> newRecords = copyOf(records.subList(0, getIndex(to)));
                    return new Segment(start, to, newRecords);
                }
            } else {
                // first sequence number of this segment if before from: start with from
                if (cl <= 0) {
                    // last sequence number of this segment is before to: end with last
                    final List<Record> newRecords = copyOf(records.subList(getIndex(from), records.size()));
                    return new Segment(from, end, newRecords);
                } else {
                    // last sequence number of this segment is after to: end with to
                    final List<Record> newRecords = copyOf(records.subList(getIndex(from), getIndex(to)));
                    return new Segment(from, to, newRecords);
                }
            }
        }

        long getByteSize() {
            return byteSize;
        }

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
    private final Map<ShardId, NavigableMap<SequenceNumber, Segment>> segments;
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
            final NavigableMap<SequenceNumber, Segment> shardCache = segments.get(shardId);
            if (shardCache == null) {
                // nothing cached for the requested shard
                return Collections.emptyList();
            }

            final SequenceNumber sequenceNumber = shardLocation.getSequenceNumber();
            final Entry<SequenceNumber, Segment> entry = shardCache.floorEntry(sequenceNumber);
            if (entry == null) {
                // no segment with requested or smaller sequence number
                return Collections.emptyList();
            }

            final Segment segment = entry.getValue();
            if (segment.getEnd().precedes(sequenceNumber)) {
                // preceding segment does not include requested sequence number
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
            final SequenceNumber sequenceNumber = shardLocation.getSequenceNumber();

            // lookup segments that immediately precede and succeed new segment to drop overlapping records
            final Segment previousSegment;
            final Segment nextSegment;
            final NavigableMap<SequenceNumber, Segment> shardCache = segments.get(shardId);
            if (shardCache == null) {
                previousSegment = null;
                nextSegment = null;
            } else {
                previousSegment = getValue(shardCache.floorEntry(sequenceNumber));
                nextSegment = getValue(shardCache.higherEntry(sequenceNumber));
            }

            final Segment segment = new Segment(sequenceNumber, records).subSegment(previousSegment, nextSegment);
            if (!segment.isEmpty()) {
                addSegment(new ShardLocation(shardId, segment.getStart()), segment);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void addSegment(ShardLocation shardLocation, Segment segment) {
        final NavigableMap<SequenceNumber, Segment> shardCache = segments.computeIfAbsent(shardLocation.getShardId(),
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
        final NavigableMap<SequenceNumber, Segment> shardCache = segments.get(shardLocation.getShardId());
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

    private static <T> T getValue(Entry<?, T> entry) {
        return entry == null ? null : entry.getValue();
    }
}
