package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.dynamodbv2.mt.mappers.DelegatingAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.util.CompositeStrings;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A streams adapter that bins and caches records of the underlying stream to allow for multiple readers to access the
 * stream. Clients generally need to read roughly the same area of the same shards at any given time for caching to be
 * effective. Lack of locality will likely result in cache misses, which in turn requires reading the underlying stream
 * which is slower and may result in throttling when DynamoDB's limit is exceeded (each shard is limited to 5 reads
 * &amp; 2 MB per second).
 *
 * <p>Current implementation maintains the following invariants about the records
 * cache:
 * <ol>
 * <li>All cached segments contain at least one record (no empty segments)</li>
 * <li>All records are cached in at most one segment (no overlapping segments)</li>
 * </ol>
 *
 * <p>Some things we may want to improve in the future:
 * <ol>
 * <li>Reduce lock contention: avoid locking all streams/shards when adding segment</li>
 * <li>Lock shard when loading records to avoid hitting throttling</li>
 * <li>Merge small adjacent segments to avoid cache fragmentation and reduce client calls</li>
 * <li>Revisit TRIM_HORIZON record caching (since the trim horizon changes over time)</li>
 * <li>Add support for LATEST.</li>
 * </ol>
 */
public class CachingAmazonDynamoDbStreams extends DelegatingAmazonDynamoDbStreams {

    private static final Logger LOG = LoggerFactory.getLogger(CachingAmazonDynamoDbStreams.class);

    /**
     * Replace with com.amazonaws.services.dynamodbv2.streamsadapter.utils.Sleeper when we upgrade
     */
    @FunctionalInterface
    interface Sleeper {

        void sleep(long millis);
    }

    /**
     * Builder for creating instances of caching streams.
     */
    public static class Builder {

        private final AmazonDynamoDBStreams amazonDynamoDbStreams;
        private Sleeper sleeper;
        private int maxIteratorCacheSize = DEFAULT_MAX_ITERATOR_CACHE_SIZE;
        private int maxRecordsCacheSize = DEFAULT_MAX_RECORDS_CACHE_SIZE;
        private int maxGetRecordsRetries = DEFAULT_MAX_GET_RECORDS_RETRIES;
        private long getRecordsLimitExceededBackoffInMillis =
            DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS;

        public Builder(AmazonDynamoDBStreams amazonDynamoDbStreams) {
            this.amazonDynamoDbStreams = amazonDynamoDbStreams;
        }

        public Builder withSleeper(Sleeper sleeper) {
            this.sleeper = sleeper;
            return this;
        }

        public Builder withMaxIteratorCacheSize(int maxIteratorCacheSize) {
            this.maxIteratorCacheSize = maxIteratorCacheSize;
            return this;
        }

        public Builder withMaxRecordsCacheSize(int maxRecordsCacheSize) {
            this.maxRecordsCacheSize = maxRecordsCacheSize;
            return this;
        }

        public Builder withMaxGetRecordsRetries(int maxGetRecordsRetries) {
            this.maxGetRecordsRetries = maxGetRecordsRetries;
            return this;
        }

        public Builder withGetRecordsLimitExceededBackoffInMillis(long getRecordsLimitExceededBackoffInMillis) {
            this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;
            return this;
        }

        /**
         * Build instance using the configured properties.
         *
         * @return a newly created {@code CachingAmazonDynamoDbStreams} based on the contents of the {@code Builder}
         */
        public CachingAmazonDynamoDbStreams build() {
            if (sleeper == null) {
                sleeper = millis -> {
                    try {
                        Thread.sleep(millis);
                    } catch (InterruptedException ie) {
                        LOG.debug("Sleeper sleep  was interrupted ", ie);
                        Thread.currentThread().interrupt();
                    }
                };
            }
            return new CachingAmazonDynamoDbStreams(
                amazonDynamoDbStreams,
                maxRecordsCacheSize,
                maxIteratorCacheSize,
                maxGetRecordsRetries,
                getRecordsLimitExceededBackoffInMillis, sleeper);
        }
    }

    /**
     * Iterator position in a DynamoDB stream shard. Facilitates comparison to other iterators and records. The position
     * is expressed as the sequence number right before the first record the iterator can retrieve. For example, an
     * iterator with position 5 retrieves records starting at 6.
     */
    private static class IteratorPosition implements Comparable<IteratorPosition>, Predicate<Record> {

        private static final BigInteger NEGATIVE_ONE = BigInteger.valueOf(-1L);
        private static final IteratorPosition TRIM_HORIZON = new IteratorPosition(ShardIteratorType.TRIM_HORIZON, null,
            NEGATIVE_ONE);

        /**
         * Creates an iterator position for a given iterator type and sequence number. TRIM_HORIZON is currently mapped
         * to the beginning of the stream. That's not technically correct, since the trim horizon in a shard may change
         * over time as records are removed. However, we handle that separately when caching iterators and records.
         */
        static IteratorPosition create(String iteratorType, String sequenceNumber) {
            switch (ShardIteratorType.fromValue(iteratorType)) {
                case TRIM_HORIZON:
                    checkArgument(sequenceNumber == null);
                    return TRIM_HORIZON;
                case AFTER_SEQUENCE_NUMBER:
                    return new IteratorPosition(ShardIteratorType.AFTER_SEQUENCE_NUMBER, sequenceNumber,
                        parseSequenceNumber(sequenceNumber));
                case AT_SEQUENCE_NUMBER:
                    return new IteratorPosition(ShardIteratorType.AT_SEQUENCE_NUMBER, sequenceNumber,
                        parseSequenceNumber(sequenceNumber).add(NEGATIVE_ONE));
                default:
                    throw new IllegalArgumentException("Unsupported shard iterator " + iteratorType);
            }
        }

        private static BigInteger parseSequenceNumber(String sequenceNumber) {
            checkArgument(sequenceNumber != null);
            BigInteger value;
            try {
                value = new BigInteger(sequenceNumber);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid sequenceNumber value", e);
            }
            checkArgument(NEGATIVE_ONE.compareTo(value) < 0, "Sequence number must be positive");
            return value;
        }

        private final ShardIteratorType shardIteratorType;
        private final String sequenceNumber;
        private final BigInteger position;

        private IteratorPosition(ShardIteratorType shardIteratorType, String sequenceNumber, BigInteger position) {
            this.shardIteratorType = shardIteratorType;
            this.sequenceNumber = sequenceNumber;
            this.position = position;
        }

        ShardIteratorType getType() {
            return shardIteratorType;
        }

        String getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public int compareTo(@Nonnull IteratorPosition o) {
            return position.compareTo(o.position);
        }

        /**
         * Iterator positions are considered equal if they refer to the same sequence number.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IteratorPosition that = (IteratorPosition) o;
            return Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(position);
        }

        /**
         * Iterators match a given record, if the record sequence number is strictly greater than its position.
         */
        @Override
        public boolean test(Record record) {
            return position.compareTo(new BigInteger(record.getDynamodb().getSequenceNumber())) < 0;
        }
    }

    /**
     * A shard iterator indicates the iterator position within a given stream and shard.
     */
    private static class ShardIterator implements Comparable<ShardIterator> {

        static final CompositeStrings compositeStrings = new CompositeStrings('/', '\\');

        static ShardIterator fromRequest(GetShardIteratorRequest request) {
            return new ShardIterator(
                request.getStreamArn(),
                request.getShardId(),
                request.getShardIteratorType(),
                request.getSequenceNumber());
        }

        static ShardIterator fromString(String external) {
            Iterator<String> it = compositeStrings.split(external);
            String streamArn = it.next();
            String shardId = it.next();
            String iteratorType = it.next();
            String sequenceNumber = it.hasNext() ? it.next() : null;
            return new ShardIterator(streamArn, shardId, iteratorType, sequenceNumber);
        }

        private final String streamArn;
        private final String shardId;
        private final IteratorPosition iteratorPosition;

        ShardIterator(String streamArn, String shardId, String iteratorType, String sequenceNumber) {
            this(streamArn, shardId, IteratorPosition.create(iteratorType, sequenceNumber));
        }

        ShardIterator(String streamArn, String shardId, IteratorPosition iteratorPosition) {
            this.streamArn = streamArn;
            this.shardId = shardId;
            this.iteratorPosition = iteratorPosition;
        }

        String getStreamArn() {
            return streamArn;
        }

        String getShardId() {
            return shardId;
        }

        IteratorPosition getIteratorPosition() {
            return iteratorPosition;
        }

        /**
         * Returns whether this iterator is immutable, i.e., does not change as records are added to the underlying
         * stream. For example, {@link ShardIteratorType#LATEST} is not immutable, since it can resolve to different
         * offsets in the stream at different times. On the other hand, {@link ShardIteratorType#AFTER_SEQUENCE_NUMBER}
         * refers to a specific position in the stream that is absolute.
         *
         * @return True if this iterator is immutable, false otherwise.
         */
        boolean isImmutable() {
            switch (iteratorPosition.getType()) {
                case TRIM_HORIZON:
                case LATEST:
                    return false;
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    return true;
                default:
                    throw new RuntimeException("Unhandled switch case");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardIterator that = (ShardIterator) o;
            return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(iteratorPosition, that.iteratorPosition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamArn, shardId, iteratorPosition);
        }

        @Override
        public int compareTo(@Nonnull ShardIterator o) {
            int c = streamArn.compareTo(o.streamArn);
            if (c != 0) {
                return c;
            }
            c = shardId.compareTo(o.shardId);
            if (c != 0) {
                return c;
            }
            return iteratorPosition.compareTo(o.iteratorPosition);
        }

        @Override
        public String toString() {
            List<String> fields = new ArrayList<>(4);
            fields.add(streamArn);
            fields.add(shardId);
            fields.add(iteratorPosition.getType().toString());
            Optional.ofNullable(iteratorPosition.getSequenceNumber()).ifPresent(fields::add);
            return compositeStrings.join(fields);
        }

        GetShardIteratorRequest toRequest() {
            return new GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(iteratorPosition.getType())
                .withSequenceNumber(iteratorPosition.getSequenceNumber());
        }

        ShardIterator next(List<Record> records) {
            return records.isEmpty()
                ? this
                : new ShardIterator(
                    streamArn,
                    shardId,
                    ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                    getLast(records).getDynamodb().getSequenceNumber());
        }
    }

    private static String toShortString(GetRecordsResult result) {
        List<Record> records = result.getRecords();
        String nextIterator = result.getNextShardIterator();
        if (records.isEmpty()) {
            return String.format("{records.size=0, nextIterator=%s}", nextIterator);
        } else {
            return String.format("{records.size=%d, first.sn=%s, last.sn=%s, nextIterator=%s",
                records.size(), records.get(0).getDynamodb().getSequenceNumber(),
                getLast(records).getDynamodb().getSequenceNumber(), nextIterator);
        }
    }

    private static final int DEFAULT_MAX_RECORDS_CACHE_SIZE = 1000;
    private static final int DEFAULT_MAX_GET_RECORDS_RETRIES = 10;
    private static final long DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS = 1000L;
    private static final int DEFAULT_MAX_ITERATOR_CACHE_SIZE = 1000;

    // cache values are quasi-immutable
    private final NavigableMap<ShardIterator, GetRecordsResult> recordsCache;
    private final Deque<ShardIterator> evictionDeque;
    private final ReadWriteLock recordsCacheLock;
    private final int maxRecordsCacheSize;
    private final int maxGetRecordsRetries;
    private final long getRecordsLimitExceededBackoffInMillis;
    private final Sleeper sleeper;
    private final LoadingCache<ShardIterator, String> iteratorCache;

    private CachingAmazonDynamoDbStreams(AmazonDynamoDBStreams amazonDynamoDbStreams,
        int maxRecordCacheSize,
        int maxIteratorCacheSize,
        int maxGetRecordsRetries,
        long getRecordsLimitExceededBackoffInMillis,
        Sleeper sleeper) {
        super(amazonDynamoDbStreams);
        this.maxRecordsCacheSize = maxRecordCacheSize;
        this.maxGetRecordsRetries = maxGetRecordsRetries;
        this.sleeper = sleeper;
        this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;
        this.recordsCache = new TreeMap<>();
        this.evictionDeque = new LinkedList<>();
        this.recordsCacheLock = new ReentrantReadWriteLock();
        this.iteratorCache = CacheBuilder
            .newBuilder()
            .maximumSize(maxIteratorCacheSize)
            .build(CacheLoader.from(this::loadShardIterator));
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        ShardIterator iterator = ShardIterator.fromRequest(request);
        // TODO add 15 min timeout? (would need to be excluded from iterator cache key)
        return new GetShardIteratorResult().withShardIterator(iterator.toString());
    }

    private String loadShardIterator(ShardIterator iterator) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cache miss for iterator {}", iterator);
        }
        return super.getShardIterator(iterator.toRequest()).getShardIterator();
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords request={}", request);
        }

        // parse iterator
        final ShardIterator iterator = ShardIterator.fromString(request.getShardIterator());

        // fetch records using cache
        final GetRecordsResult loadedResult = getRecords(iterator);

        // apply limit if applicable
        final GetRecordsResult result = applyLimit(request.getLimit(), iterator, loadedResult);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords result={}", toShortString(result));
        }

        return result;
    }

    /**
     * Gets records for the given shard iterator position using the record and iterator cache.
     *
     * @param iterator Position in the a given stream shard for which to retrieve records
     * @return Results loaded from the cache or underlying stream
     */
    private GetRecordsResult getRecords(ShardIterator iterator) {
        int getRecordsRetries = 0;
        while (getRecordsRetries < maxGetRecordsRetries) {
            // first check cache for matches (both exact and overlap)
            final Lock readLock = recordsCacheLock.readLock();
            readLock.lock();
            try {
                final Optional<GetRecordsResult> cachedRecordsResult = getCacheSegment(iterator);
                if (cachedRecordsResult.isPresent()) {
                    return cachedRecordsResult.get();
                }
            } finally {
                readLock.unlock();
            }

            // If nothing found, get stream iterator (hopefully from cache)
            final String shardIterator = getIterator(iterator);

            // then load records from stream
            final GetRecordsResult loadedRecordsResult;
            try {
                loadedRecordsResult = super.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));
            } catch (LimitExceededException e) {
                long backoff = (getRecordsRetries + 1) * getRecordsLimitExceededBackoffInMillis;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("GetRecords limit exceeded for iterator {} on retry attempt {}; backing off for {}.",
                        iterator, getRecordsRetries, backoff);
                }
                sleeper.sleep(backoff);
                getRecordsRetries++;
                continue;
            } catch (ExpiredIteratorException e) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Underlying iterator {} for cached iterator {} expired.", shardIterator, iterator);
                }
                removeIterator(iterator);
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Loaded getRecords result={}", toShortString(loadedRecordsResult));
            }

            // if we didn't load anything, return without adding cache segment (preserves non-empty range invariant)
            if (loadedRecordsResult.getRecords().isEmpty()) {
                if (loadedRecordsResult.getNextShardIterator() == null) {
                    return loadedRecordsResult;
                }

                // replace with loaded iterator, so it is used to proceed through stream on next call
                addIterator(iterator, loadedRecordsResult.getNextShardIterator());
                return new GetRecordsResult()
                    .withRecords(loadedRecordsResult.getRecords())
                    .withNextShardIterator(iterator.toString());
            }

            // otherwise (if we found records), try to update the cache
            final GetRecordsResult result;
            final Lock writeLock = recordsCacheLock.writeLock();
            writeLock.lock();
            try {
                // since we did not hold the lock while loading records, it is possible that the cache was updated
                // in the meantime, so we first check the cache again. If we find a a result we use it. We may have
                // loaded additional records not contained in that cached segment, so we could technically try to
                // squeeze them in after the segment we found, but for now we just throw them away.
                result = getCacheSegment(iterator)
                    .orElseGet(() -> alignAndAddCacheSegment(iterator, loadedRecordsResult));
            } finally {
                writeLock.unlock();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning {} records with next iterator {}.",
                    result.getRecords().size(), result.getNextShardIterator());
            }
            return result;
        }
        throw new LimitExceededException("Exhausted GetRecords retry limit.");
    }

    /**
     * Reduces the result based on the limit if present.
     *
     * @param limit Limit specified in the request
     * @param iterator Iterator specified in the request
     * @param loadedResult Loaded result to limit
     * @return Result that is limited to the number of records specified in the request
     */
    private GetRecordsResult applyLimit(Integer limit, ShardIterator iterator, GetRecordsResult loadedResult) {
        checkArgument(limit == null || limit > 0);
        final GetRecordsResult result;
        if (limit == null || limit >= loadedResult.getRecords().size()) {
            result = loadedResult;
        } else {
            List<Record> records = loadedResult.getRecords().subList(0, limit);
            result = new GetRecordsResult()
                .withRecords(records)
                .withNextShardIterator(iterator.next(records).toString());
        }
        return result;
    }

    /**
     * Must be called with cache lock held.
     *
     * @param iterator Iterator for which to retrieve matching records from the cache
     * @return List of matching (i.e., immediately succeeding iterator) cached records or empty list if none match
     */
    private Optional<GetRecordsResult> getCacheSegment(ShardIterator iterator) {
        final Optional<GetRecordsResult> cachedRecordsResult;
        final Map.Entry<ShardIterator, GetRecordsResult> floorCacheEntry = recordsCache.floorEntry(iterator);
        if (floorCacheEntry == null) {
            // an invariant preserved by this method is that we never put an empty collection
            // into the cache, so we can use an empty list to indicate a cache miss
            cachedRecordsResult = Optional.empty();
        } else {
            ShardIterator lowerIterator = floorCacheEntry.getKey();
            GetRecordsResult lowerResult = floorCacheEntry.getValue();
            if (lowerIterator.compareTo(iterator) == 0) {
                // exact iterator hit (hopefully common case), return all cached records
                cachedRecordsResult = Optional.of(lowerResult);
            } else if (lowerIterator.getStreamArn().equals(iterator.getStreamArn())
                && lowerIterator.getShardId().equals(iterator.getShardId())
                && iterator.getIteratorPosition().test(getLast(lowerResult.getRecords()))) {
                // Cache entry contains records that match (i.e., come after) the requested iterator position. Filter
                // cached records to those that match. Return only that subset, to increase the chance of using a shared
                // iterator position on the next getRecords call.
                final List<Record> matchingCachedRecords = lowerResult.getRecords().stream()
                    .filter(iterator.getIteratorPosition())
                    .collect(toList());
                cachedRecordsResult = Optional.of(new GetRecordsResult()
                    .withRecords(matchingCachedRecords)
                    .withNextShardIterator(iterator.next(matchingCachedRecords).toString()));
            } else {
                // no cached records in the preceding cache entry match the requested position (i.e., all records
                // precede it)
                cachedRecordsResult = Optional.empty();
            }
        }
        return cachedRecordsResult;
    }

    /**
     * Must be called with cache lock held. Aligns the given loaded result by filtering out records that overlap with
     * subsequent segments and adds it to the cache under the given iterator key.
     *
     * @param iterator Iterator for which to store the cache entry
     * @param loadedRecordsResult Result loaded from stream that is to be stored in the cache
     * @return Result aligned with other segments and stored in cache
     */
    private GetRecordsResult alignAndAddCacheSegment(ShardIterator iterator, GetRecordsResult loadedRecordsResult) {
        final GetRecordsResult result;
        // to preserve the invariant of non-overlapping segments, we need to check the next higher segment
        final Map.Entry<ShardIterator, GetRecordsResult> higherCacheEntry = recordsCache.higherEntry(iterator);
        final List<Record> loadedRecords = loadedRecordsResult.getRecords();
        if (higherCacheEntry == null
            || !higherCacheEntry.getKey().getStreamArn().equals(iterator.getStreamArn())
            || !higherCacheEntry.getKey().getShardId().equals(iterator.getShardId())
            || !higherCacheEntry.getKey().getIteratorPosition().test(getLast(loadedRecords))) {
            // if there is no higher cache entry, or there is, but it doesn't overlap (i.e., its shard
            // iterator does not match any of the loaded records), cache and return all loaded records.
            final String loadedNextIterator = loadedRecordsResult.getNextShardIterator();
            if (loadedNextIterator == null) {
                result = loadedRecordsResult;
            } else {
                ShardIterator nextIterator = iterator.next(loadedRecords);
                addIterator(nextIterator, loadedNextIterator);
                result = new GetRecordsResult()
                    .withRecords(loadedRecords)
                    .withNextShardIterator(nextIterator.toString());
            }
            addCacheSegment(iterator, result);
        } else {
            // otherwise (there is higher cache entry), filter the set of records to cache and return
            // to only those that don't overlap, to retain proper binning of ranges
            final ShardIterator higherIterator = higherCacheEntry.getKey();
            final List<Record> nonOverlappingRecords = loadedRecords.stream()
                .filter(higherIterator.getIteratorPosition().negate())
                .collect(toList());
            if (nonOverlappingRecords.isEmpty()) {
                // all records we loaded are contained in next segment, but iterators don't align:
                // merge segments by removing old segment and caching its records under new iterator.
                // Not caching loaded next iterator, since next segment may not overlap perfectly and we
                // presumably already cached its next iterator when we loaded it.
                replaceCacheSegment(higherIterator, iterator);
                result = higherCacheEntry.getValue();
            } else {
                // There are some loaded records that are not in the next segment and some that are:
                // cache a new segment for the non-overlapping records. Not caching loaded next iterator,
                // since it falls somewhere into the next segment.
                result = new GetRecordsResult()
                    .withRecords(nonOverlappingRecords)
                    .withNextShardIterator(iterator.next(nonOverlappingRecords).toString());
                addCacheSegment(iterator, result);
            }
        }
        return result;
    }

    /**
     * Must be called with cache lock held.
     *
     * @param iterator key under which to cache the segment
     * @param result result to cache
     */
    private void addCacheSegment(ShardIterator iterator, GetRecordsResult result) {
        GetRecordsResult prevResult = recordsCache.put(iterator, result);
        checkState(prevResult == null);

        // evict if necessary - FIFO in terms of when entries were added to the cache
        evictionDeque.addLast(iterator);
        while (evictionDeque.size() > maxRecordsCacheSize) {
            ShardIterator oldest = evictionDeque.removeFirst();
            recordsCache.remove(oldest);
        }
    }

    /**
     * Must be called with cache lock held.
     *
     * @param iterator key under which segment is currently store
     * @param newIterator key under which the segment should be stored
     */
    private void replaceCacheSegment(ShardIterator iterator, ShardIterator newIterator) {
        GetRecordsResult result = recordsCache.remove(iterator);
        checkState(result != null);
        recordsCache.put(newIterator, result);
    }

    private String getIterator(ShardIterator iterator) {
        return iterator.isImmutable()
            ? iteratorCache.getUnchecked(iterator)
            : loadShardIterator(iterator);
    }

    private void addIterator(ShardIterator iterator, String loadedIterator) {
        if (iterator.isImmutable()) {
            iteratorCache.put(iterator, loadedIterator);
        }
    }

    private void removeIterator(ShardIterator iterator) {
        if (iterator.isImmutable()) {
            iteratorCache.invalidate(iterator);
        }
    }

}
