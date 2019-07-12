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
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
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

        private static final int DEFAULT_MAX_RECORD_BYTES_CACHED = 100 * 1024 * 1024;
        private static final int DEFAULT_MAX_GET_RECORDS_RETRIES = 10;
        private static final long DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS = 1000L;
        private static final int DEFAULT_MAX_ITERATOR_CACHE_SIZE = 1000;
        private static final long DEFAULT_DESCRIBE_STREAM_CACHE_TTL = 5L;
        private static final long DEFAULT_MAX_DESCRIBE_STREAM_CACHE_SHARD_COUNT = 5000L;
        private static final boolean DESCRIBE_STREAM_CACHE_ENABLED = true;
        private static final long DEFAULT_EMPTY_RESULT_CACHE_TTL_IN_MILLIS = 100L;

        private final AmazonDynamoDBStreams amazonDynamoDbStreams;
        private MeterRegistry meterRegistry;
        private Sleeper sleeper;
        private Ticker ticker;
        private long maxRecordsByteSize = DEFAULT_MAX_RECORD_BYTES_CACHED;
        private int maxIteratorCacheSize = DEFAULT_MAX_ITERATOR_CACHE_SIZE;
        private int maxGetRecordsRetries = DEFAULT_MAX_GET_RECORDS_RETRIES;
        private long describeStreamCacheTtl = DEFAULT_DESCRIBE_STREAM_CACHE_TTL;
        private boolean describeStreamCacheEnabled = DESCRIBE_STREAM_CACHE_ENABLED;
        private long maxDescribeStreamCacheWeight = DEFAULT_MAX_DESCRIBE_STREAM_CACHE_SHARD_COUNT;
        private long getRecordsLimitExceededBackoffInMillis =
            DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS;
        private long emptyResultCacheTtlInMillis = DEFAULT_EMPTY_RESULT_CACHE_TTL_IN_MILLIS;

        public Builder(AmazonDynamoDBStreams amazonDynamoDbStreams) {
            this.amazonDynamoDbStreams = amazonDynamoDbStreams;
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
         * Maximum number of retries if {@link LimitExceededException}s are encountered when loading records from the
         * underlying stream into the cache.
         *
         * @param maxGetRecordsRetries Maximum number of retries.
         * @return This Builder.
         */
        public Builder withMaxGetRecordsRetries(int maxGetRecordsRetries) {
            checkArgument(maxGetRecordsRetries >= 0);
            this.maxGetRecordsRetries = maxGetRecordsRetries;
            return this;
        }

        /**
         * Backoff time for each retry after a {@link LimitExceededException} is caught while loading records from the
         * underlying stream into the cache.
         *
         * @param getRecordsLimitExceededBackoffInMillis Backoff time in millis.
         * @return This Builder.
         */
        public Builder withGetRecordsLimitExceededBackoffInMillis(long getRecordsLimitExceededBackoffInMillis) {
            checkArgument(getRecordsLimitExceededBackoffInMillis >= 0);
            this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;
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
         * @param emptyResultCacheTtlInMillis   Time in milliseconds for how long to cache and return empty results.
         * @return This Builder
         */
        public Builder withEmptyResultCacheTtlInMillis(long emptyResultCacheTtlInMillis) {
            checkArgument(emptyResultCacheTtlInMillis > 0);
            this.emptyResultCacheTtlInMillis = emptyResultCacheTtlInMillis;
            return this;
        }

        /** MeterRegistry to record metrics to.
         *
         * @param meterRegistry Meter registry to report metrics to.
         * @return This Builder.
         */
        public Builder withMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
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
            if (ticker == null) {
                ticker = Ticker.systemTicker();
            }
            return new CachingAmazonDynamoDbStreams(
                amazonDynamoDbStreams,
                meterRegistry == null ? new CompositeMeterRegistry() : meterRegistry,
                sleeper,
                ticker,
                maxRecordsByteSize,
                maxGetRecordsRetries,
                getRecordsLimitExceededBackoffInMillis,
                maxIteratorCacheSize,
                describeStreamCacheTtl,
                maxDescribeStreamCacheWeight,
                describeStreamCacheEnabled,
                emptyResultCacheTtlInMillis);
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
                request.getStreamArn(),
                request.getShardId(),
                ShardIteratorType.fromValue(request.getShardIteratorType()),
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

            return new CachingShardIterator(streamArn, shardId, type, sequenceNumber, dynamoDbIterator);
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
        private final String streamArn;
        @Nonnull
        private final String shardId;
        @Nullable
        private final String sequenceNumber;
        @Nullable
        private final String dynamoDbIterator;

        private CachingShardIterator(
            @Nonnull String streamArn,
            @Nonnull String shardId,
            @Nonnull ShardIteratorType type,
            @Nullable String sequenceNumber,
            @Nullable String dynamoDbIterator) {
            this.streamArn = checkNotNull(streamArn);
            this.shardId = checkNotNull(shardId);
            this.type = type;

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
                    return Optional.of(StreamShardPosition.at(streamArn, shardId, sequenceNumber));
                case AFTER_SEQUENCE_NUMBER:
                    return Optional.of(StreamShardPosition.after(streamArn, shardId, sequenceNumber));
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
            return StreamShardPosition.at(streamArn, shardId, record);
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
            return new CachingShardIterator(streamArn, shardId, type, sequenceNumber, dynamoDbIterator);
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
            return new CachingShardIterator(streamArn, shardId, AFTER_SEQUENCE_NUMBER,
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
                .withStreamArn(streamArn)
                .withShardId(shardId)
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
            fields.add(shardId);
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
            return Objects.requireNonNullElse(dynamoDbIterator, streamArn)
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
            return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && type == that.type
                && Objects.equals(sequenceNumber, that.sequenceNumber)
                && Objects.equals(dynamoDbIterator, that.dynamoDbIterator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamArn, shardId, type, sequenceNumber, dynamoDbIterator);
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
     * @param l1    First list. Must not be null, but may be empty.
     * @param l2    Second list. Must not be null, but may be empty.
     * @param <T>   Type of elements stored in lists
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

    // configuration properties
    private final Sleeper sleeper;
    private final int maxGetRecordsRetries;
    private final long getRecordsLimitExceededBackoffInMillis;

    // locks to sequence access to shards
    private final StreamsRecordCache recordCache;
    private final Striped<Lock> positionLoadLocks;
    private final Cache<StreamShardPosition, Boolean> emptyResultCache;

    // iterator cache
    private final LoadingCache<CachingShardIterator, String> iteratorCache;

    private final Cache<String, DescribeStreamResult> describeStreamCache;
    private final boolean describeStreamCacheEnabled;

    @VisibleForTesting
    // TODO: introduce builder/constructor variant that allows passing in cache reference
    public Cache<String, DescribeStreamResult> getDescribeStreamCache() {
        return describeStreamCache;
    }

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
    private final Counter getRecordsUncached;
    private final Timer getShardIteratorLoadTime;
    private final Counter getShardIteratorUncached;

    private CachingAmazonDynamoDbStreams(AmazonDynamoDBStreams amazonDynamoDbStreams,
                                         MeterRegistry meterRegistry,
                                         Sleeper sleeper,
                                         Ticker ticker,
                                         long maxRecordsByteSize,
                                         int maxGetRecordsRetries,
                                         long getRecordsLimitExceededBackoffInMillis,
                                         int maxIteratorCacheSize,
                                         long describeStreamCacheTtl,
                                         long maxDescribeStreamCacheWeight,
                                         boolean describeStreamCacheEnabled,
                                         long emptyResultCacheTtlInMillis) {
        super(amazonDynamoDbStreams);
        this.sleeper = sleeper;
        this.maxGetRecordsRetries = maxGetRecordsRetries;
        this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;

        final Weigher<String, DescribeStreamResult> weightByShardCount = (key, result) -> {
            List<Shard> shards = result.getStreamDescription().getShards();
            return shards == null ? 0 : shards.size();
        };
        this.describeStreamCacheEnabled = describeStreamCacheEnabled;
        this.describeStreamCache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(describeStreamCacheTtl, TimeUnit.SECONDS)
            .maximumWeight(maxDescribeStreamCacheWeight)
            .weigher(weightByShardCount)
            .ticker(ticker)
            .recordStats()
            .build();

        this.recordCache = new StreamsRecordCache(meterRegistry, maxRecordsByteSize);
        this.positionLoadLocks = Striped.lazyWeakLock(16384);
        this.emptyResultCache = CacheBuilder.newBuilder()
            .expireAfterWrite(emptyResultCacheTtlInMillis, TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .recordStats()
            .build();

        this.iteratorCache = CacheBuilder.newBuilder()
            .maximumSize(maxIteratorCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadShardIterator));

        final String cn = CachingAmazonDynamoDbStreams.class.getSimpleName();
        this.getRecordsTime = meterRegistry.timer(cn + ".GetRecords.Time");
        this.getRecordsSize = meterRegistry.summary(cn + ".GetRecords.Size");
        this.getRecordsLoadTime = meterRegistry.timer(cn + ".GetRecords.Load.Time");
        this.getRecordsLoadSize = meterRegistry.summary(cn + ".GetRecords.Load.Size");
        this.getRecordsLoadLockWaitTime = meterRegistry.timer(cn + ".GetRecords.Load.Lock.Time");
        this.getRecordsLoadLockTimeouts = meterRegistry.counter(cn + ".GetRecords.Load.Lock.Timeouts");
        this.getRecordsLoadRetries = meterRegistry.summary(cn + ".GetRecords.Load.Retries");
        this.getRecordsLoadMaxRetries = meterRegistry.counter(cn + ".GetRecords.Load.MaxRetries");
        this.getRecordsLoadExpiredIterator = meterRegistry.counter(cn + ".GetRecords.Load.ExpiredIterator");
        this.getRecordsUncached = meterRegistry.counter(cn + ".GetRecords.Uncached");
        this.getShardIteratorLoadTime = meterRegistry.timer(cn + ".GetShardIterator.Load.Time");
        this.getShardIteratorUncached = meterRegistry.counter(cn + ".GetShardIterator.Uncached");
        GuavaCacheMetrics.monitor(meterRegistry, iteratorCache, cn + ".GetShardIterator");
        GuavaCacheMetrics.monitor(meterRegistry, emptyResultCache, cn + ".EmptyResult");
        GuavaCacheMetrics.monitor(meterRegistry, describeStreamCache, cn + ".DescribeStream");
    }

    /**
     * Gets the {@code DescribeStreamResult} from the DescribeStream API.
     * @param describeStreamRequest Describe stream request.
     *
     * @return Stream details for the given request with all the shards currently available.
     *      Throws exceptions that AmazonDynamoDBStreams describeStream could potentially throw.
     * @throws ResourceNotFoundException
     *         The requested resource could not be found. The stream might not be specified correctly.
     * @throws LimitExceededException
     *         The requested resource exceeds the maximum number allowed, or the number of concurrent stream requests
     *         exceeds the maximum number allowed (5).
     */
    protected DescribeStreamResult loadStreamDescriptionForAllShards(DescribeStreamRequest describeStreamRequest)
        throws AmazonDynamoDBException, ResourceNotFoundException {

        List<Shard> allShards = new ArrayList<>();
        String streamArn = describeStreamRequest.getStreamArn();
        String lastShardId = describeStreamRequest.getExclusiveStartShardId();

        if (LOG.isDebugEnabled() && describeStreamCacheEnabled) {
            LOG.debug("cache miss in describe stream cache for key: {}", streamArn);
        }
        // TODO metrics: count cache miss
        // TODO metrics: measure time for fetching results from describeStream

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
                // Catch exceptions thrown on cache lookup or cache loader and try load method once more. (get call
                // will throw UncheckedExecutionException before aws exception (i.e. AmazonDynamoDBException)).
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

    private String loadShardIterator(CachingShardIterator iterator) {
        return loadShardIterator(iterator.toRequest());
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
            final CachingShardIterator iterator = CachingShardIterator.fromExternalString(request.getShardIterator());
            final Optional<StreamShardPosition> positionOpt = iterator.resolvePosition();
            final GetRecordsResult result;
            if (positionOpt.isPresent()) {
                final StreamShardPosition position = positionOpt.get();

                // first check if we got an empty result recently for this position
                if (Boolean.TRUE == emptyResultCache.getIfPresent(position)) {
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
        final Lock lock = positionLoadLocks.get(position);
        // try to acquire a lock for the given shard position (to avoid redundant concurrent load attempts)
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
                if (Boolean.TRUE == emptyResultCache.getIfPresent(position)) {
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
                LOG.warn("Failed to obtain record load lock for iterator {}.", iterator);
            }
            getRecordsLoadLockTimeouts.increment();
            if (Boolean.TRUE == emptyResultCache.getIfPresent(position)) {
                result = iterator.emptyResult();
            } else {
                final List<Record> cachedRecords = recordCache.getRecords(position, limit);
                result = cachedRecords.isEmpty()
                    ? iterator.emptyResult() // Could throw LimitExceededException here instead
                    : iterator.nextResult(cachedRecords);
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
        while (getRecordsRetries < maxGetRecordsRetries) {
            // first get the physical DynamoDB iterator
            final String dynamoDbIterator = iterator.getDynamoDbIterator()
                .orElseGet(() -> {
                    try {
                        return iteratorCache.getUnchecked(iterator);
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
                long backoff = (getRecordsRetries + 1) * getRecordsLimitExceededBackoffInMillis;
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
                iterator.resolvePosition().ifPresent(position -> emptyResultCache.put(position, Boolean.TRUE));
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
