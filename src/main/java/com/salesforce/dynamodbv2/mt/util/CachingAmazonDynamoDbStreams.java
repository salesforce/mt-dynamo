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
import com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.salesforce.dynamodbv2.mt.mappers.DelegatingAmazonDynamoDbStreams;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
        private static final int DEFAULT_MAX_ITERATOR_CACHE_SIZE = 100;

        private final AmazonDynamoDBStreams amazonDynamoDbStreams;
        private Sleeper sleeper;
        private long maxRecordsByteSize = DEFAULT_MAX_RECORD_BYTES_CACHED;
        private int maxIteratorCacheSize = DEFAULT_MAX_ITERATOR_CACHE_SIZE;
        private int maxGetRecordsRetries = DEFAULT_MAX_GET_RECORDS_RETRIES;
        private long getRecordsLimitExceededBackoffInMillis =
            DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS;

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
         * @return this Builder.
         */
        public Builder withMaxIteratorCacheSize(int maxIteratorCacheSize) {
            this.maxIteratorCacheSize = maxIteratorCacheSize;
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
                sleeper,
                maxRecordsByteSize,
                maxGetRecordsRetries,
                getRecordsLimitExceededBackoffInMillis,
                maxIteratorCacheSize);
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

        // derived cached state
        private final SequenceNumber parsedSequenceNumber;

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
                    this.parsedSequenceNumber = null;
                    this.dynamoDbIterator = dynamoDbIterator;
                    break;
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    checkArgument(sequenceNumber != null);
                    checkArgument(dynamoDbIterator == null);
                    this.sequenceNumber = sequenceNumber;
                    this.parsedSequenceNumber = SequenceNumber.fromRawValue(sequenceNumber);
                    this.dynamoDbIterator = null;
                    break;
                default:
                    throw new RuntimeException("Missing case statement for ShardIteratorType");
            }
        }

        ShardId getShardUid() {
            return new ShardId(streamArn, shardId);
        }

        Optional<ShardLocation> resolveLocation() {
            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    return Optional.empty();
                case AT_SEQUENCE_NUMBER:
                    return Optional.of(new ShardLocation(getShardUid(), parsedSequenceNumber));
                case AFTER_SEQUENCE_NUMBER:
                    return Optional.of(new ShardLocation(getShardUid(), parsedSequenceNumber.next()));
                default:
                    throw new RuntimeException("Unhandled switch case");
            }
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
         * @param records Records list.
         * @return New shard iterator that starts after the last record in the list.
         */
        CachingShardIterator nextShardIterator(List<Record> records) {
            return records.isEmpty() ? this : new CachingShardIterator(streamArn, shardId, AFTER_SEQUENCE_NUMBER,
                getLast(records).getDynamodb().getSequenceNumber(), null);
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
            CachingShardIterator that = (CachingShardIterator) o;
            return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && type == that.type
                && Objects.equals(sequenceNumber, that.sequenceNumber)
                && Objects.equals(dynamoDbIterator, that.dynamoDbIterator)
                && Objects.equals(parsedSequenceNumber, that.parsedSequenceNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamArn, shardId, type, sequenceNumber, dynamoDbIterator, parsedSequenceNumber);
        }
    }

    private static class CachingGetRecordsResult {
        private final List<Record> records;
        private final CachingShardIterator nextShardIterator;

        CachingGetRecordsResult(List<Record> records, CachingShardIterator nextShardIterator) {
            this.records = records;
            this.nextShardIterator = nextShardIterator;
        }

        List<Record> getRecords() {
            return records;
        }

        CachingShardIterator getNextShardIterator() {
            return nextShardIterator;
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

    // configuration properties
    private final Sleeper sleeper;
    private final int maxGetRecordsRetries;
    private final long getRecordsLimitExceededBackoffInMillis;

    // locks to sequence access to shards
    private final Striped<Lock> shardLocks;
    private final RecordsCache recordCache;

    // iterator cache
    private final LoadingCache<CachingShardIterator, String> iteratorCache;

    private CachingAmazonDynamoDbStreams(AmazonDynamoDBStreams amazonDynamoDbStreams,
                                         Sleeper sleeper,
                                         long maxRecordsByteSize,
                                         int maxGetRecordsRetries,
                                         long getRecordsLimitExceededBackoffInMillis,
                                         int maxIteratorCacheSize) {
        super(amazonDynamoDbStreams);
        this.sleeper = sleeper;
        this.maxGetRecordsRetries = maxGetRecordsRetries;
        this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;

        this.shardLocks = Striped.lazyWeakLock(1000);
        this.recordCache = new RecordsCache(maxRecordsByteSize);

        this.iteratorCache = CacheBuilder
            .newBuilder()
            .maximumSize(maxIteratorCacheSize)
            .build(CacheLoader.from(this::loadShardIterator));
    }

    private String loadShardIterator(CachingShardIterator iterator) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cache miss for iterator {}", iterator);
        }
        return super.getShardIterator(iterator.toRequest()).getShardIterator();
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
                dynamoDbIterator = dynamoDbStreams.getShardIterator(request).getShardIterator();
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

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords request={}", request);
        }

        final String iteratorString = request.getShardIterator();
        checkArgument(iteratorString != null);
        final int limit = Optional.ofNullable(request.getLimit()).orElse(GET_RECORDS_LIMIT);
        checkArgument(limit > 0 && limit <= GET_RECORDS_LIMIT);

        // parse iterator
        final CachingShardIterator iterator = CachingShardIterator.fromExternalString(request.getShardIterator());

        // Lock the shard to avoid (1) exceeding streams reader limit (See
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-dynamodb-streams)
        // and (2) concurrent readers from fetching overlapping segments unnecessarily
        final ShardId shardId = iterator.getShardUid();
        final Lock lock = shardLocks.get(shardId);
        try {
            if (!lock.tryLock(5000, TimeUnit.SECONDS)) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("getRecords failed to acquire lock for shard ", shardId);
                }
                throw new LimitExceededException("Failed to acquire shard lock within max time.");
            }
        } catch (InterruptedException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("getRecords interrupted trying to acquire lock for shard ", shardId);
            }
            Thread.currentThread().interrupt();
        }

        final List<Record> records = new ArrayList<>(limit);
        final CachingShardIterator nextIterator;
        try {
            // fetch records from cache
            iterator.resolveLocation()
                .map(location -> recordCache.getRecords(location, limit))
                .ifPresent(records::addAll);

            // check if we got enough
            if (records.size() < limit) {
                // not enough cached records: read segment from underlying shard
                final CachingShardIterator cachedNextIterator = iterator.nextShardIterator(records);
                final CachingGetRecordsResult loadedResult = getRecords(cachedNextIterator);

                final List<Record> loadedRecords = loadedResult.getRecords();
                final CachingShardIterator loadedNextIterator = loadedResult.getNextShardIterator();

                if (loadedRecords.isEmpty()) {
                    // no records loaded: update iterator
                    nextIterator = records.isEmpty() ? loadedNextIterator : cachedNextIterator;
                } else {
                    // some records loaded: update record cache and result

                    // update cache
                    final ShardLocation location = cachedNextIterator.resolveLocation()
                        .orElseGet(() -> new ShardLocation(shardId, SequenceNumber.fromRecord(loadedRecords.get(0))));
                    recordCache.putRecords(location, loadedRecords);

                    // update result records and next iterator
                    final int remaining = limit - records.size();
                    if (loadedRecords.size() > remaining) {
                        // more records loaded than needed
                        records.addAll(loadedRecords.subList(0, remaining));
                        nextIterator = iterator.nextShardIterator(records);
                    } else {
                        records.addAll(loadedRecords);
                        nextIterator = loadedNextIterator;
                    }
                }
            } else {
                // full cache hit: simply return cached records and next (lazy) iterator
                nextIterator = iterator.nextShardIterator(records);
            }
        } finally {
            lock.unlock();
        }

        final GetRecordsResult result = new GetRecordsResult()
            .withRecords(records)
            .withNextShardIterator(nextIterator == null ? null : nextIterator.toExternalString());

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
    private CachingGetRecordsResult getRecords(CachingShardIterator iterator) {
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

            // next load records from stream
            final GetRecordsRequest request = new GetRecordsRequest().withShardIterator(dynamoDbIterator);
            final GetRecordsResult result;
            try {
                result = dynamoDbStreams.getRecords(request);
            } catch (LimitExceededException e) {
                long backoff = (getRecordsRetries + 1) * getRecordsLimitExceededBackoffInMillis;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("getRecords limit exceeded: iterator={}, retry attempt={}, backoff={}.", iterator,
                        getRecordsRetries, backoff);
                }
                sleeper.sleep(backoff);
                getRecordsRetries++;
                continue;
            } catch (ExpiredIteratorException e) {
                // if we loaded the iterator from our cache, reload it
                if (iterator.getDynamoDbIterator().isEmpty()) {
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
                LOG.debug("getRecords loaded records: result={}, iterator={}", toShortString(result), iterator);
            }

            // compute next iterator
            final String loadedNextIterator = result.getNextShardIterator();
            final List<Record> records = result.getRecords();
            final CachingShardIterator nextIterator;
            if (loadedNextIterator == null) {
                nextIterator = null;
            } else {
                if (records.isEmpty() && iterator.getDynamoDbIterator().isPresent()) {
                    nextIterator = iterator.withDynamoDbIterator(loadedNextIterator);
                } else {
                    nextIterator = iterator.nextShardIterator(records);
                    iteratorCache.put(nextIterator, loadedNextIterator);
                }
            }

            return new CachingGetRecordsResult(records, nextIterator);
        }

        if (LOG.isWarnEnabled()) {
            LOG.warn("GetRecords exceeded maximum number of retries");
        }
        throw new LimitExceededException("Exhausted GetRecords retry limit.");
    }

}
