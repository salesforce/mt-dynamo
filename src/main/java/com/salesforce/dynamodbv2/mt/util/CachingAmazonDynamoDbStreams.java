package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getLast;
import static com.salesforce.dynamodbv2.mt.util.ShardIterator.ITERATOR_SEPARATOR;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.salesforce.dynamodbv2.mt.mappers.DelegatingAmazonDynamoDbStreams;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * </ol>
 */
public class CachingAmazonDynamoDbStreams extends DelegatingAmazonDynamoDbStreams {

    /**
     * Replace with com.amazonaws.services.dynamodbv2.streamsadapter.utils.Sleeper when we upgrade.
     */
    @FunctionalInterface
    interface Sleeper {

        void sleep(long millis);

    }

    /**
     * Replace with com.amazonaws.services.dynamodbv2.streamsadapter.utils.ThreadSleeper when we upgrade.
     */
    private static class ThreadSleeper implements Sleeper {
        @Override
        public void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ie) {
                LOG.debug("Sleeper sleep  was interrupted ", ie);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Builder for creating instances of caching streams.
     */
    public static class Builder {

        private static final int DEFAULT_MAX_RECORD_BYTES_CACHED = 100 * 1024 * 1024;
        private static final int DEFAULT_GET_RECORDS_MAX_RETRIES = 10;
        private static final long DEFAULT_GET_RECORDS_BACKOFF_IN_MILLIS = 1000L;
        private static final int DEFAULT_MAX_ITERATOR_CACHE_SIZE = 1000;
        private static final long DEFAULT_DESCRIBE_STREAM_CACHE_TTL = 5L;
        private static final long DEFAULT_MAX_DESCRIBE_STREAM_CACHE_SHARD_COUNT = 5000L;
        private static final boolean DESCRIBE_STREAM_CACHE_ENABLED = true;
        private static final long DEFAULT_EMPTY_RESULT_CACHE_TTL_IN_MILLIS = 1000L;
        private static final long DEFAULT_TRIM_HORIZON_ITERATOR_CACHE_TTL_IN_SECONDS = 60;

        private final AmazonDynamoDBStreams amazonDynamoDbStreams;
        private MeterRegistry meterRegistry;
        private String metricPrefix;
        private Sleeper sleeper;
        private Ticker ticker;
        private Cache<String, DescribeStreamResult> describeStreamCache;
        private long maxRecordsByteSize = DEFAULT_MAX_RECORD_BYTES_CACHED;
        private int maxIteratorCacheSize = DEFAULT_MAX_ITERATOR_CACHE_SIZE;
        private int getRecordsMaxRetries = DEFAULT_GET_RECORDS_MAX_RETRIES;
        private long describeStreamCacheTtl = DEFAULT_DESCRIBE_STREAM_CACHE_TTL;
        private boolean describeStreamCacheEnabled = DESCRIBE_STREAM_CACHE_ENABLED;
        private long maxDescribeStreamCacheWeight = DEFAULT_MAX_DESCRIBE_STREAM_CACHE_SHARD_COUNT;
        private long getRecordsBackoffInMillis = DEFAULT_GET_RECORDS_BACKOFF_IN_MILLIS;
        private long emptyResultCacheTtlInMillis = DEFAULT_EMPTY_RESULT_CACHE_TTL_IN_MILLIS;
        private long trimHorizonIteratorCacheTtlInSeconds = DEFAULT_TRIM_HORIZON_ITERATOR_CACHE_TTL_IN_SECONDS;
        // for testing
        private Striped<Lock> getRecordsLocks;

        public Builder(AmazonDynamoDBStreams amazonDynamoDbStreams) {
            this.amazonDynamoDbStreams = amazonDynamoDbStreams;
        }

        /**
         * MeterRegistry to record metrics to.
         *
         * @param meterRegistry Meter registry to report metrics to.
         * @return This Builder.
         */
        public Builder withMeterRegistry(MeterRegistry meterRegistry, String metricPrefix) {
            this.meterRegistry = meterRegistry;
            this.metricPrefix = metricPrefix;
            return this;
        }

        /**
         * Sleep function to use for retry backoff. Defaults to {@link Thread#sleep(long)}.
         *
         * @param sleeper Sleeper implementation.
         * @return This Builder.
         */
        public Builder withSleeper(Sleeper sleeper) {
            this.sleeper = sleeper;
            return this;
        }

        /**
         * Ticker used for cache expiration. Defaults to {@link Ticker#systemTicker()}.
         *
         * @param ticker Ticker implementation
         * @return This Builder
         */
        public Builder withTicker(Ticker ticker) {
            this.ticker = ticker;
            return this;
        }

        /**
         * The maximum total sum of {@link StreamRecord#getSizeBytes()} the cache may hold. This is an approximation for
         * heap size. The actual {@link GetRecordsResult} objects stored in the cache carry additional overhead, so this
         * value should be used as a rough guideline.
         *
         * @param maxRecordsByteSize Maximum cache size in sum of record bytes.
         * @return This Builder.
         */
        public Builder withMaxRecordsByteSize(long maxRecordsByteSize) {
            checkArgument(maxRecordsByteSize >= 0);
            this.maxRecordsByteSize = maxRecordsByteSize;
            return this;
        }

        /**
         * Maximum number of retries if {@link LimitExceededException}s are encountered when loading records from the
         * underlying stream into the cache.
         *
         * @param getRecordsMaxRetries Maximum number of retries.
         * @return This Builder.
         */
        public Builder withGetRecordsMaxRetries(int getRecordsMaxRetries) {
            checkArgument(getRecordsMaxRetries >= 0);
            this.getRecordsMaxRetries = getRecordsMaxRetries;
            return this;
        }

        /**
         * Backoff time for each retry after a {@link LimitExceededException} is caught while loading records from the
         * underlying stream into the cache.
         *
         * @param getRecordsBackoffInMillis Backoff time in millis.
         * @return This Builder.
         */
        public Builder withGetRecordsBackoffInMillis(long getRecordsBackoffInMillis) {
            checkArgument(getRecordsBackoffInMillis >= 0);
            this.getRecordsBackoffInMillis = getRecordsBackoffInMillis;
            return this;
        }

        /**
         * Maximum number of shard iterators to cache.
         *
         * @param maxIteratorCacheSize Maximum number of iterators to cache.
         * @return This Builder.
         */
        public Builder withMaxIteratorCacheSize(int maxIteratorCacheSize) {
            this.maxIteratorCacheSize = maxIteratorCacheSize;
            return this;
        }

        /**
         * Cache to use for describe stream calls.
         *
         * @param describeStreamCache The cache to use for describe stream
         * @return this Builder.
         */
        public Builder withDescribeStreamCache(Cache describeStreamCache) {
            this.describeStreamCache = describeStreamCache;
            return this;
        }

        /**
         * Enables or disables the describe stream cache.
         *
         * @param describeStreamCacheEnabled Disable or enable the describe stream cache.
         * @return this Builder.
         */
        public Builder withDescribeStreamCacheEnabled(boolean describeStreamCacheEnabled) {
            this.describeStreamCacheEnabled = describeStreamCacheEnabled;
            return this;
        }

        /**
         * Time to live for describe stream cache entries.
         *
         * @param describeStreamCacheTtl Duration to wait before refreshing describe stream cache on writes.
         *                               Duration has seconds as the unit when the cache is created.
         * @return this Builder.
         */
        public Builder withDescribeStreamCacheTtl(long describeStreamCacheTtl) {
            this.describeStreamCacheTtl = describeStreamCacheTtl;
            return this;
        }

        /**
         * The maximum total number of shards the describe stream cache may hold for a given key.
         *
         * @param maxDescribeStreamCacheWeight Maximum number of shards allowed for value in describe stream cache.
         * @return This Builder.
         */
        public Builder withMaxDescribeStreamCacheWeight(long maxDescribeStreamCacheWeight) {
            checkArgument(maxDescribeStreamCacheWeight >= 0);
            this.maxDescribeStreamCacheWeight = maxDescribeStreamCacheWeight;
            return this;
        }

        /**
         * The time empty results should be cached and returned before attempting to load records from the stream at the
         * given position again.
         *
         * @param emptyResultCacheTtlInMillis Time in milliseconds for how long to cache and return empty results.
         * @return This Builder
         */
        public Builder withEmptyResultCacheTtlInMillis(long emptyResultCacheTtlInMillis) {
            checkArgument(emptyResultCacheTtlInMillis > 0);
            this.emptyResultCacheTtlInMillis = emptyResultCacheTtlInMillis;
            return this;
        }

        /**
         * The time mappings from trim horizon iterators to sequence number iterators should be cached.
         *
         * @param trimHorizonIteratorCacheTtlInSeconds Time in seconds to cache trim horizon iterator mappings.
         * @return This Builder.
         */
        public Builder withTrimHorizonIteratorCacheTtlInSeconds(long trimHorizonIteratorCacheTtlInSeconds) {
            this.trimHorizonIteratorCacheTtlInSeconds = trimHorizonIteratorCacheTtlInSeconds;
            return this;
        }

        @VisibleForTesting
        Builder withGetRecordsLocks(Striped<Lock> getRecordsLocks) {
            this.getRecordsLocks = getRecordsLocks;
            return this;
        }

        /**
         * Build instance using the configured properties.
         *
         * @return a newly created {@code CachingAmazonDynamoDbStreams} based on the contents of the {@code Builder}
         */
        public CachingAmazonDynamoDbStreams build() {
            final Sleeper sleeper = this.sleeper == null ? new ThreadSleeper() : this.sleeper;
            final Ticker ticker = this.ticker == null ? Ticker.systemTicker() : this.ticker;
            final MeterRegistry meterRegistry = this.meterRegistry == null ? new CompositeMeterRegistry()
                : this.meterRegistry;

            return new CachingAmazonDynamoDbStreams(
                amazonDynamoDbStreams,
                sleeper,
                meterRegistry,
                metricPrefix,
                describeStreamCache == null ? CacheBuilder
                    .newBuilder()
                    .expireAfterWrite(describeStreamCacheTtl, TimeUnit.SECONDS)
                    .maximumWeight(maxDescribeStreamCacheWeight)
                    .<String, DescribeStreamResult>weigher((s, r) -> r.getStreamDescription().getShards().size())
                    .ticker(ticker)
                    .recordStats()
                    .build() : this.describeStreamCache,
                describeStreamCacheEnabled,
                new StreamsRecordCache(meterRegistry, maxRecordsByteSize),
                CacheBuilder.newBuilder()
                    .expireAfterWrite(emptyResultCacheTtlInMillis, TimeUnit.MILLISECONDS)
                    .ticker(ticker)
                    .recordStats()
                    .build(),
                getRecordsLocks == null ? Striped.lazyWeakLock(16384) : getRecordsLocks,
                getRecordsMaxRetries,
                getRecordsBackoffInMillis,
                CacheBuilder.newBuilder()
                    .maximumSize(maxIteratorCacheSize)
                    .recordStats()
                    .build(),
                CacheBuilder.newBuilder()
                    .expireAfterWrite(trimHorizonIteratorCacheTtlInSeconds, TimeUnit.SECONDS)
                    .ticker(ticker)
                    .recordStats()
                    .build()
            );
        }
    }

    /**
     * A logical shard iterator that optionally wraps an underlying DynamoDB iterator.
     */
    private static final class CachingShardIterator {

        private static final CompositeStrings compositeStrings = new CompositeStrings('/', '\\');

        /**
         * Returns an iterator for the given request and optional DynamoDB iterator.
         *
         * @param request          Iterator request.
         * @param dynamoDbIterator DynamoDB iterator (optional).
         * @return Logical shard iterator.
         */
        static CachingShardIterator fromRequest(GetShardIteratorRequest request, @Nullable String dynamoDbIterator) {
            return new CachingShardIterator(
                ShardIteratorType.fromValue(request.getShardIteratorType()),
                request.getStreamArn(),
                request.getShardId(),
                request.getSequenceNumber(),
                dynamoDbIterator
            );
        }

        /**
         * Parses a CachingShardIterator instance from its external String representation.
         *
         * @param value External string form.
         * @return CachingShardIterator instance.
         */
        static CachingShardIterator fromExternalString(String value) {
            ShardIterator iterator = ShardIterator.fromString(value);
            String streamArn = iterator.getArn();

            String rest = iterator.getRest();
            int idx = rest.lastIndexOf(ITERATOR_SEPARATOR);
            String dynamoDbIterator;
            if (idx == -1) {
                dynamoDbIterator = null;
            } else {
                dynamoDbIterator = iterator.withRest(rest.substring(0, idx++)).toString();
                rest = rest.substring(idx);
            }

            Iterator<String> it = compositeStrings.split(rest);
            String shardId = it.next();
            String pointer = it.next();
            ShardIteratorType type;
            String sequenceNumber;
            if (TRIM_HORIZON.toString().equals(pointer)) {
                type = TRIM_HORIZON;
                sequenceNumber = null;
            } else if (LATEST.toString().equals(pointer)) {
                type = LATEST;
                sequenceNumber = null;
            } else if (pointer.startsWith(AFTER_SEQUENCE_NUMBER.toString())) {
                type = AFTER_SEQUENCE_NUMBER;
                sequenceNumber = getSequenceNumber(pointer, AFTER_SEQUENCE_NUMBER);
            } else if (pointer.startsWith(AT_SEQUENCE_NUMBER.toString())) {
                type = AT_SEQUENCE_NUMBER;
                sequenceNumber = getSequenceNumber(pointer, AT_SEQUENCE_NUMBER);
            } else {
                throw new IllegalArgumentException("Invalid position segment in shard iterator string " + value);
            }

            return new CachingShardIterator(type, streamArn, shardId, sequenceNumber, dynamoDbIterator);
        }

        private static String getSequenceNumber(String pointer, ShardIteratorType type) {
            int length = type.toString().length();
            checkArgument(pointer.length() > length + 2);
            checkArgument(pointer.charAt(length) == '.');
            return pointer.substring(length + 1);
        }

        @Nonnull
        private final ShardIteratorType type;
        @Nonnull
        private final StreamShardId streamShardId;
        @Nullable
        private final String sequenceNumber;
        @Nullable
        private final String dynamoDbIterator;  // note: excluded from equality and hash code

        private CachingShardIterator(@Nonnull ShardIteratorType type,
                                     @Nonnull String streamArn,
                                     @Nonnull String shardId,
                                     @Nullable String sequenceNumber,
                                     @Nullable String dynamoDbIterator) {
            this(type, new StreamShardId(streamArn, shardId), sequenceNumber, dynamoDbIterator);
        }

        private CachingShardIterator(
            @Nonnull ShardIteratorType type,
            @Nonnull StreamShardId streamShardId,
            @Nullable String sequenceNumber,
            @Nullable String dynamoDbIterator) {
            this.streamShardId = checkNotNull(streamShardId);
            this.type = checkNotNull(type);
            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    checkArgument(sequenceNumber == null);
                    checkArgument(dynamoDbIterator != null);
                    this.sequenceNumber = null;
                    this.dynamoDbIterator = dynamoDbIterator;
                    break;
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    checkArgument(sequenceNumber != null);
                    checkArgument(dynamoDbIterator == null);
                    this.sequenceNumber = sequenceNumber;
                    this.dynamoDbIterator = null;
                    break;
                default:
                    throw new RuntimeException("Missing case statement for ShardIteratorType");
            }
        }

        @Nonnull
        public ShardIteratorType getType() {
            return type;
        }

        @Nonnull
        public StreamShardId getStreamShardId() {
            return streamShardId;
        }

        /**
         * Resolves the absolute position of this iterator. Only non-empty if iterator is immutable, i.e., refers to
         * fixed sequence number in the stream shard.
         *
         * @return Absolute position of this iterator or empty if this iterator does not have a fixed position.
         */
        Optional<StreamShardPosition> resolvePosition() {
            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    return Optional.empty();
                case AT_SEQUENCE_NUMBER:
                    return Optional.of(StreamShardPosition.at(streamShardId, sequenceNumber));
                case AFTER_SEQUENCE_NUMBER:
                    return Optional.of(StreamShardPosition.after(streamShardId, sequenceNumber));
                default:
                    throw new RuntimeException("Unhandled switch case");
            }
        }

        /**
         * Resolves the position of an iterator that starts at the given record.
         *
         * @param record Record for which to resolve the position.
         * @return Position of record.
         */
        StreamShardPosition resolvePosition(Record record) {
            return StreamShardPosition.at(streamShardId, record);
        }

        /**
         * Returns a new virtual shard iterator positioned at the sequence number of the first record in the list.
         *
         * @param records Record list. Must not be empty.
         * @return New shard iterator positioned at the first record in the list.
         */
        CachingShardIterator resolvePositionIterator(List<Record> records) {
            assert !records.isEmpty();
            return new CachingShardIterator(AT_SEQUENCE_NUMBER, streamShardId,
                records.get(0).getDynamodb().getSequenceNumber(), null);
        }

        /**
         * Returns the DynamoDB shard iterator, may be empty.
         *
         * @return DynamoDB shard iterator
         */
        Optional<String> getDynamoDbIterator() {
            return Optional.ofNullable(dynamoDbIterator);
        }

        /**
         * Returns a new iterator with the given dynamoDbIterator.
         *
         * @param dynamoDbIterator DynamoDb iterator.
         * @return A new iterator.
         */
        CachingShardIterator withDynamoDbIterator(String dynamoDbIterator) {
            return new CachingShardIterator(type, streamShardId, sequenceNumber, dynamoDbIterator);
        }

        /**
         * Returns a new virtual shard iterator that starts at the sequence number immediately after the last record in
         * the given records list.
         *
         * @param records Records list. Must not be empty.
         * @return New shard iterator that starts after the last record in the list.
         */
        CachingShardIterator nextShardIterator(List<Record> records) {
            assert !records.isEmpty();
            return new CachingShardIterator(AFTER_SEQUENCE_NUMBER, streamShardId,
                getLast(records).getDynamodb().getSequenceNumber(), null);
        }

        /**
         * Returns a result object for the given records that starts with a next iterator positioned after the last
         * record.
         *
         * @param records Record list. Must not be empty.
         * @return GetRecordsResult.
         */
        GetRecordsResult nextResult(List<Record> records) {
            assert !records.isEmpty();
            return new GetRecordsResult()
                .withRecords(records)
                .withNextShardIterator(nextShardIterator(records).toExternalString());
        }

        /**
         * Returns an empty result object with this as the next shard iterator.
         *
         * @return Empty result object
         */
        GetRecordsResult emptyResult() {
            return new GetRecordsResult().withRecords().withNextShardIterator(toExternalString());
        }

        /**
         * Returns an iterator request that can be used to retrieve an iterator from DynamoDB.
         *
         * @return Iterator request.
         */
        GetShardIteratorRequest toRequest() {
            return new GetShardIteratorRequest()
                .withStreamArn(streamShardId.getStreamArn())
                .withShardId(streamShardId.getShardId())
                .withShardIteratorType(type)
                .withSequenceNumber(sequenceNumber);
        }

        /**
         * Serializes this CachingShardIterator into its external string format.
         *
         * @return Externalized string.
         */
        String toExternalString() {
            List<String> fields = new ArrayList<>(4);
            fields.add(streamShardId.getShardId());
            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    fields.add(type.toString());
                    break;
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    fields.add(String.format("%s.%s", type.toString(), sequenceNumber));
                    break;
                default:
                    throw new RuntimeException("Unhandled case in switch statement");
            }
            return Objects.requireNonNullElse(dynamoDbIterator, streamShardId.getStreamArn())
                + ITERATOR_SEPARATOR + compositeStrings.join(fields);
        }

        @Override
        public String toString() {
            return toExternalString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final CachingShardIterator that = (CachingShardIterator) o;
            return type == that.type
                && Objects.equals(streamShardId, that.streamShardId)
                && Objects.equals(sequenceNumber, that.sequenceNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, streamShardId, sequenceNumber);
        }
    }

    // logger instance
    private static final Logger LOG = LoggerFactory.getLogger(CachingAmazonDynamoDbStreams.class);

    // DynamoDB's GetRecords limit. Constant defined in AmazonDynamoDBStreamsAdapterClient (but not on classpath).
    static final int GET_RECORDS_LIMIT = 1000;

    /**
     * Returns a shortened string representation of the given GetRecordsResult intended for debug logs that doesn't
     * include individual records.
     *
     * @param result Result to format.
     * @return Shortened string representation of the given GetRecordsResult.
     */
    private static String toShortString(GetRecordsResult result) {
        List<Record> records = result.getRecords();
        String nextIterator = result.getNextShardIterator();
        if (records.isEmpty()) {
            return String.format("{records.size=0, nextIterator=%s}", nextIterator);
        } else {
            return String.format("{records.size=%d, first.sn=%s, last.sn=%s, nextIterator=%s}",
                records.size(), records.get(0).getDynamodb().getSequenceNumber(),
                getLast(records).getDynamodb().getSequenceNumber(), nextIterator);
        }
    }

    /**
     * Concatenates the two lists into one.
     *
     * @param l1  First list. Must not be null, but may be empty.
     * @param l2  Second list. Must not be null, but may be empty.
     * @param <T> Type of elements stored in lists
     * @return Combined list
     */
    private static <T> List<T> concat(List<T> l1, List<T> l2) {
        if (l1.isEmpty()) {
            return l2;
        }
        if (l2.isEmpty()) {
            return l1;
        }
        final List<T> l = new ArrayList<>(l1.size() + l2.size());
        l.addAll(l1);
        l.addAll(l2);
        return l;
    }

    private final Sleeper sleeper;

    // describeStream cache
    private final Cache<String, DescribeStreamResult> describeStreamCache;
    private final boolean describeStreamCacheEnabled;

    // getRecords caches and configuration
    private final StreamsRecordCache recordCache;
    private final Cache<StreamShardPosition, Boolean> getRecordsEmptyResultCache;
    private final Striped<Lock> getRecordsLocks;
    private final int getRecordsMaxRetries;
    private final long getRecordsBackoffInMillis;

    // getShardIterator cache
    private final Cache<CachingShardIterator, String> iteratorCache;
    private final Cache<StreamShardId, CachingShardIterator> trimHorizonCache;

    // meters for observability
    private final Timer getRecordsTime;
    private final DistributionSummary getRecordsSize;
    private final Timer getRecordsLoadTime;
    private final Timer getRecordsLoadLockWaitTime;
    private final DistributionSummary getRecordsLoadSize;
    private final DistributionSummary getRecordsLoadRetries;
    private final Counter getRecordsLoadMaxRetries;
    private final Counter getRecordsLoadExpiredIterator;
    private final Counter getRecordsLoadLockTimeouts;
    private final Counter getRecordsLoadLockTimeoutsFailures;
    private final Counter getRecordsUncached;
    private final Timer getShardIteratorLoadTime;
    private final Counter getShardIteratorUncached;

    @VisibleForTesting
    CachingAmazonDynamoDbStreams(AmazonDynamoDBStreams amazonDynamoDbStreams,
                                 Sleeper sleeper,
                                 MeterRegistry meterRegistry,
                                 String metricPrefix,
                                 Cache<String, DescribeStreamResult> describeStreamCache,
                                 boolean describeStreamCacheEnabled,
                                 StreamsRecordCache recordCache,
                                 Cache<StreamShardPosition, Boolean> getRecordsEmptyResultCache,
                                 Striped<Lock> getRecordsLocks,
                                 int getRecordsMaxRetries,
                                 long getRecordsBackoffInMillis,
                                 Cache<CachingShardIterator, String> iteratorCache,
                                 Cache<StreamShardId, CachingShardIterator> trimHorizonCache) {
        super(amazonDynamoDbStreams);
        this.sleeper = sleeper;
        this.describeStreamCache = describeStreamCache;

        this.describeStreamCacheEnabled = describeStreamCacheEnabled;
        this.recordCache = recordCache;
        this.getRecordsEmptyResultCache = getRecordsEmptyResultCache;
        this.getRecordsLocks = getRecordsLocks;
        this.getRecordsMaxRetries = getRecordsMaxRetries;
        this.getRecordsBackoffInMillis = getRecordsBackoffInMillis;
        this.iteratorCache = iteratorCache;
        this.trimHorizonCache = trimHorizonCache;

        // eagerly create various meters
        final String cn = CachingAmazonDynamoDbStreams.class.getSimpleName();
        final String prefix = StringUtils.isNullOrEmpty(metricPrefix) ? cn : cn + "." + metricPrefix;
        this.getRecordsTime = meterRegistry.timer(prefix + ".GetRecords.Time");
        this.getRecordsSize = meterRegistry.summary(prefix + ".GetRecords.Size");
        this.getRecordsLoadTime = meterRegistry.timer(prefix + ".GetRecords.Load.Time");
        this.getRecordsLoadSize = meterRegistry.summary(prefix + ".GetRecords.Load.Size");
        this.getRecordsLoadLockWaitTime = meterRegistry.timer(prefix + ".GetRecords.Load.Lock.Time");
        this.getRecordsLoadLockTimeouts = meterRegistry.counter(prefix + ".GetRecords.Load.Lock.Timeouts");
        this.getRecordsLoadLockTimeoutsFailures = meterRegistry.counter(prefix
            + ".GetRecords.Load.Lock.Timeouts.Failures");
        this.getRecordsLoadRetries = meterRegistry.summary(prefix + ".GetRecords.Load.Retries");
        this.getRecordsLoadMaxRetries = meterRegistry.counter(prefix + ".GetRecords.Load.MaxRetries");
        this.getRecordsLoadExpiredIterator = meterRegistry.counter(prefix + ".GetRecords.Load.ExpiredIterator");
        this.getRecordsUncached = meterRegistry.counter(prefix + ".GetRecords.Uncached");
        this.getShardIteratorLoadTime = meterRegistry.timer(prefix + ".GetShardIterator.Load.Time");
        this.getShardIteratorUncached = meterRegistry.counter(prefix + ".GetShardIterator.Uncached");

        GuavaCacheMetrics.monitor(meterRegistry, iteratorCache, prefix + ".GetShardIterator");
        GuavaCacheMetrics.monitor(meterRegistry, getRecordsEmptyResultCache, prefix + ".EmptyResult");
        GuavaCacheMetrics.monitor(meterRegistry, describeStreamCache, prefix + ".DescribeStream");
        GuavaCacheMetrics.monitor(meterRegistry, trimHorizonCache, prefix + ".TrimHorizon");
    }

    /**
     * Gets the {@code DescribeStreamResult} from the DescribeStream API.
     *
     * @param describeStreamRequest Describe stream request
     * @return Stream details for the given request with all the shards currently available
     * @throws ResourceNotFoundException The requested resource could not be found. The stream might not be specified
     *                                   correctly
     * @throws LimitExceededException    The requested resource exceeds the maximum number allowed, or the number of
     *                                   concurrent stream requests exceeds the maximum number allowed (5)
     */
    protected DescribeStreamResult loadStreamDescriptionForAllShards(DescribeStreamRequest describeStreamRequest)
        throws AmazonDynamoDBException, ResourceNotFoundException {

        List<Shard> allShards = new ArrayList<>();
        String streamArn = describeStreamRequest.getStreamArn();
        String lastShardId = describeStreamRequest.getExclusiveStartShardId();

        if (LOG.isDebugEnabled() && describeStreamCacheEnabled) {
            LOG.debug("cache miss in describe stream cache for key: {}", streamArn);
        }

        DescribeStreamResult result;
        do {
            describeStreamRequest = new DescribeStreamRequest().withStreamArn(streamArn)
                .withExclusiveStartShardId(lastShardId);
            result = super.describeStream(describeStreamRequest);
            if (result.getStreamDescription().getShards() != null
                && !result.getStreamDescription().getShards().isEmpty()) {
                allShards.addAll(result.getStreamDescription().getShards());
            }
            lastShardId = result.getStreamDescription().getLastEvaluatedShardId();
        } while (lastShardId != null && !lastShardId.isEmpty());

        return new DescribeStreamResult().withStreamDescription(result.getStreamDescription().withShards(allShards));
    }

    /**
     * Gets {@code DescribeStreamResult} for request from cache or describeStream API.
     *
     * @param describeStreamRequest Describe stream request.
     * @return {@code DescribeStreamResult}.
     */
    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest)
        throws AmazonDynamoDBException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("describeStream request={}", describeStreamRequest);
        }

        if (describeStreamCacheEnabled) {
            String key = describeStreamRequest.getStreamArn();
            try {
                return describeStreamCache.get(key,
                    () -> this.loadStreamDescriptionForAllShards(describeStreamRequest));
            } catch (UncheckedExecutionException | ExecutionException | InvalidCacheLoadException e) {
                // Catch exceptions thrown on cache lookup or cache loader and try load method once more. (Get call
                // will throw UncheckedExecutionException before AWS exception (i.e., AmazonDynamoDBException)).
                LOG.warn("Failed getting DescribeStreamResult for all shards from cache lookup or load method.  "
                    + "Retrying describeStream call. " + e.getMessage());
                return this.loadStreamDescriptionForAllShards(describeStreamRequest);
            }
        }

        // Call describeStream without caching since the cache is disabled
        return this.loadStreamDescriptionForAllShards(describeStreamRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        // We only retrieve an actual stream iterator for mutable types (LATEST and TRIM_HORIZON). For immutable
        // iterators (AT|AFTER_SEQUENCE_NUMBER) we retrieve stream iterators lazily, since we may not need one at all
        // if we have a records cache hit. We cannot be lazy for mutable iterators, since that may violate client
        // expectations: for example, if a client gets a LATEST shard iterator, then inserts items, and then gets
        // records, it expects to (eventually) see them. If we were to defer looking up the iterator until get records
        // is called, LATEST will resolve to a different position in the stream, so the client would not see records for
        // the items that were inserted. In either case we include the original request in the iterator we return such
        // that getRecords can parse it, so that we can cache the get records result (without the request context, we
        // would not know which stream, shard, and position we retrieved records for).
        String dynamoDbIterator;
        switch (ShardIteratorType.fromValue(request.getShardIteratorType())) {
            case TRIM_HORIZON:
                /*
                 * Check first if we recently found records at TRIM_HORIZON and if so, return a virtual iterator at the
                 * sequence number of the first record. There is a risk with this approach that the TRIM_HORIZON moves
                 * by the time the iterator is used, i.e., what we returned as the offset to use is now trimmed. If we
                 * do not have records cached at the returned offset, the call to load records will result in a
                 * TrimmedDataAccessException. This case should be rather unlikely, because we only cache the mapping
                 * for a short time (so the likelihood that records are trimmed is low and the likelihood that records
                 * are cached is high), AFAIK, DynamoDB currently trims by deleting shards (not by removing records from
                 * shards), and KCL retries when it hits this exception (it's common to encounter it in local DynamoDB
                 * due to a similar situation). Nevertheless, if we run into unexpected problems, we may need to remove
                 * this cache or store some more information in the virtual iterator that allows us to retry
                 * transparently when encountering a TrimmedDataAccessException.
                 */
                CachingShardIterator cachedIterator =
                    trimHorizonCache.getIfPresent(new StreamShardId(request.getStreamArn(), request.getShardId()));
                if (cachedIterator != null) {
                    return new GetShardIteratorResult().withShardIterator(cachedIterator.toExternalString());
                } else {
                    dynamoDbIterator = loadShardIterator(request);
                    getShardIteratorUncached.increment();
                }
                break;
            case LATEST:
                dynamoDbIterator = loadShardIterator(request);
                getShardIteratorUncached.increment();
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                dynamoDbIterator = null;
                break;
            default:
                throw new RuntimeException("Missing switch case on ShardIteratorType");
        }
        CachingShardIterator iterator = CachingShardIterator.fromRequest(request, dynamoDbIterator);
        return new GetShardIteratorResult().withShardIterator(iterator.toExternalString());
    }

    private String loadShardIterator(GetShardIteratorRequest request) {
        return getShardIteratorLoadTime.record(() -> dynamoDbStreams.getShardIterator(request)).getShardIterator();
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        return getRecordsTime.record(() -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getRecords request={}", request);
            }

            final String iteratorString = request.getShardIterator();
            checkArgument(iteratorString != null);
            final int limit = Optional.ofNullable(request.getLimit()).orElse(GET_RECORDS_LIMIT);
            checkArgument(limit > 0 && limit <= GET_RECORDS_LIMIT);

            // parse iterator
            final CachingShardIterator parsed = CachingShardIterator.fromExternalString(request.getShardIterator());
            final CachingShardIterator iterator = parsed.type == TRIM_HORIZON
                ? Optional.ofNullable(trimHorizonCache.getIfPresent(parsed.getStreamShardId())).orElse(parsed)
                : parsed;

            final Optional<StreamShardPosition> positionOpt = iterator.resolvePosition();
            final GetRecordsResult result;
            if (positionOpt.isPresent()) {
                final StreamShardPosition position = positionOpt.get();

                // first check if we got an empty result recently for this position
                if (Boolean.TRUE == getRecordsEmptyResultCache.getIfPresent(position)) {
                    result = iterator.emptyResult();
                } else {
                    // then check the record cache
                    final List<Record> cachedRecords = recordCache.getRecords(position, limit);
                    if (cachedRecords.isEmpty()) {
                        // cache miss: try to fetch records from stream
                        result = loadRecordsAtPosition(iterator, limit);
                    } else if (cachedRecords.size() < limit) {
                        // partial cache hit: try to fetch more records
                        final CachingShardIterator nextIterator = iterator.nextShardIterator(cachedRecords);
                        final int remaining = limit - cachedRecords.size();
                        final GetRecordsResult loadedResult = loadRecordsAtPosition(nextIterator, remaining);
                        result = loadedResult.withRecords(concat(cachedRecords, loadedResult.getRecords()));
                    } else {
                        // full cache hit: return cached records (without fetching more from stream)
                        assert cachedRecords.size() == limit;
                        result = iterator.nextResult(cachedRecords);
                    }
                }
            } else {
                // not currently caching iterators without fixed position: fetch records
                getRecordsUncached.increment();
                result = loadRecords(iterator, limit);
            }

            getRecordsSize.record(result.getRecords().size());

            if (LOG.isDebugEnabled()) {
                LOG.debug("getRecords result={}", toShortString(result));
            }

            return result;
        });
    }

    private GetRecordsResult loadRecordsAtPosition(CachingShardIterator iterator, int limit) {
        final GetRecordsResult result;
        final StreamShardPosition position = iterator.resolvePosition().orElseThrow();
        final Lock lock = getRecordsLocks.get(position);
        // try to acquire a lock for the given shard position (to avoid redundant concurrent load attempts)
        // since we are accessing an external resource while holding the lock, limit the max wait time
        if (getRecordsLoadLockWaitTime.record(() -> {
            try {
                return lock.tryLock(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted waiting on load records lock");
            }
        })) {
            // got the lock: check again to see if caches have been updated in the meantime, otherwise load
            try {
                if (Boolean.TRUE == getRecordsEmptyResultCache.getIfPresent(position)) {
                    // another thread loaded an empty result while this one was waiting on lock
                    result = iterator.emptyResult();
                } else {
                    final List<Record> cachedRecords = recordCache.getRecords(position, limit);
                    result = cachedRecords.isEmpty()
                        ? loadRecords(iterator, limit) // still nothing cached: load from the shard
                        : iterator.nextResult(cachedRecords); // otherwise return what was loaded while waiting on lock
                }
            } finally {
                lock.unlock();
            }
        } else {
            // failed to get load lock: check caches again to see if anything was loaded while waiting, but don't load.
            if (LOG.isWarnEnabled()) {
                LOG.warn("Failed to obtain getRecords lock for iterator {}.", iterator);
            }
            getRecordsLoadLockTimeouts.increment();
            if (Boolean.TRUE == getRecordsEmptyResultCache.getIfPresent(position)) {
                result = iterator.emptyResult();
            } else {
                final List<Record> cachedRecords = recordCache.getRecords(position, limit);
                if (cachedRecords.isEmpty()) {
                    getRecordsLoadLockTimeoutsFailures.increment();
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Failed to obtain getRecords lock or cached records for iterator {}.", iterator);
                    }
                    throw new LimitExceededException("Failed to obtain getRecords lock in time");
                } else {
                    return iterator.nextResult(cachedRecords);
                }
            }
        }
        return result;
    }

    /**
     * Gets records for the given shard iterator position using the record and iterator cache.
     *
     * @param iterator Position in the a given stream shard for which to retrieve records
     * @return Results loaded from the cache or underlying stream
     */
    private GetRecordsResult loadRecords(CachingShardIterator iterator, int limit) {
        int getRecordsRetries = 0;
        while (getRecordsRetries < getRecordsMaxRetries) {
            // first get the physical DynamoDB iterator
            final String dynamoDbIterator = iterator.getDynamoDbIterator()
                .orElseGet(() -> {
                    try {
                        return iteratorCache.get(iterator, () -> loadShardIterator(iterator.toRequest()));
                    } catch (ExecutionException e) {
                        Throwables.throwIfUnchecked(e.getCause());
                        throw new UncheckedExecutionException(e);
                    } catch (UncheckedExecutionException e) {
                        Throwables.throwIfUnchecked(e.getCause());
                        throw e;
                    }
                });

            // next load records from stream (always load MAX to minimize calls via cache; apply limit after)
            final GetRecordsRequest request = new GetRecordsRequest().withShardIterator(dynamoDbIterator);
            final GetRecordsResult result;
            try {
                result = getRecordsLoadTime.record(() -> dynamoDbStreams.getRecords(request));
            } catch (LimitExceededException e) {
                long backoff = (getRecordsRetries + 1) * getRecordsBackoffInMillis;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("loadRecords limit exceeded: iterator={}, retry attempt={}, backoff={}.", iterator,
                        getRecordsRetries, backoff);
                }
                sleeper.sleep(backoff);
                getRecordsRetries++;
                continue;
            } catch (ExpiredIteratorException e) {
                // if we loaded the iterator from our cache, reload it
                if (iterator.getDynamoDbIterator().isEmpty()) {
                    getRecordsLoadExpiredIterator.increment();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Cached iterator expired: iterator={}, expired={}.", iterator, dynamoDbIterator);
                    }
                    iteratorCache.invalidate(iterator);
                    continue;
                }
                // otherwise, make the client obtain a new iterator as usual
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("loadRecords: result={}, iterator={}", toShortString(result), iterator);
            }

            final String loadedNextIterator = result.getNextShardIterator();
            final List<Record> loadedRecords = result.getRecords();

            getRecordsLoadSize.record(loadedRecords.size());

            // compute next iterator and update iterator and records caches
            final CachingShardIterator nextIterator;
            if (loadedRecords.isEmpty()) {
                // update iterator cache
                if (loadedNextIterator == null) {
                    nextIterator = null;
                } else {
                    if (iterator.getDynamoDbIterator().isEmpty()) {
                        // update cache with new physical iterator to increase chances of making progress
                        nextIterator = iterator;
                        iteratorCache.put(nextIterator, loadedNextIterator);
                    } else {
                        // use new physical iterator next time to continue to progress
                        nextIterator = iterator.withDynamoDbIterator(loadedNextIterator);
                    }
                }

                // update record cache: record empty result (to avoid concurrent requests)
                iterator.resolvePosition()
                    .ifPresent(position -> getRecordsEmptyResultCache.put(position, Boolean.TRUE));
            } else {
                // update iterator cache
                if (loadedNextIterator == null) {
                    nextIterator = null;
                } else {
                    nextIterator = iterator.nextShardIterator(loadedRecords);
                    iteratorCache.put(nextIterator, loadedNextIterator);
                }

                // update records cache (either under iterator position or first record sequence number)
                final StreamShardPosition location = iterator.resolvePosition()
                    .orElseGet(() -> iterator.resolvePosition(loadedRecords.get(0)));
                recordCache.putRecords(location, loadedRecords);

                // remember TRIM_HORIZON location for a short time, so subsequent requests can leverage the cache
                if (iterator.getType() == TRIM_HORIZON) {
                    trimHorizonCache.put(iterator.getStreamShardId(), iterator.resolvePositionIterator(loadedRecords));
                }
            }

            getRecordsLoadRetries.record(getRecordsRetries);
            // compute result
            return loadedRecords.size() > limit
                ? iterator.nextResult(loadedRecords.subList(0, limit))
                : new GetRecordsResult()
                .withRecords(loadedRecords)
                .withNextShardIterator(nextIterator == null ? null : nextIterator.toExternalString());
        }

        getRecordsLoadRetries.record(getRecordsRetries);
        getRecordsLoadMaxRetries.increment();
        if (LOG.isWarnEnabled()) {
            LOG.warn("GetRecords exceeded maximum number of retries");
        }
        throw new LimitExceededException("Exhausted GetRecords retry limit.");
    }

}
