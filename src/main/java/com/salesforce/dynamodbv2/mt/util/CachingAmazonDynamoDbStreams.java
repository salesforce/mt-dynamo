package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getLast;
import static java.math.BigInteger.ONE;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.mappers.DelegatingAmazonDynamoDbStreams;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import java.util.function.Predicate;
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

        private static final int DEFAULT_MAX_RECORD_BYTES_CACHED = 1000;
        private static final int DEFAULT_MAX_GET_RECORDS_RETRIES = 10;
        private static final long DEFAULT_GET_RECORDS_LIMIT_EXCEEDED_BACKOFF_IN_MILLIS = 1000L;

        private final AmazonDynamoDBStreams amazonDynamoDbStreams;
        private Sleeper sleeper;
        private long maxRecordsByteSize = DEFAULT_MAX_RECORD_BYTES_CACHED;
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
         */
        public Builder withMaxRecordsByteSize(long maxRecordsByteSize) {
            this.maxRecordsByteSize = maxRecordsByteSize;
            return this;
        }

        /**
         * Sleep function to use for retry backoff. Defaults to {@link Thread#sleep(long)}.
         */
        public Builder withSleeper(Sleeper sleeper) {
            this.sleeper = sleeper;
            return this;
        }

        /**
         * Maximum number of retries if {@link LimitExceededException}s are encountered when loading records from the
         * underlying stream into the cache.
         */
        public Builder withMaxGetRecordsRetries(int maxGetRecordsRetries) {
            this.maxGetRecordsRetries = maxGetRecordsRetries;
            return this;
        }

        /**
         * Backoff time for each retry after a {@link LimitExceededException} is caught while loading records from the
         * underlying stream into the cache.
         */
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
                sleeper,
                maxRecordsByteSize,
                maxGetRecordsRetries,
                getRecordsLimitExceededBackoffInMillis);
        }
    }

    /**
     * An absolute position in a stream expressed as a triple of streamArn, shardId, and sequence number. Note: there is
     * no guarantee that there is actually a record at the given position in the stream.
     */
    private static final class IteratorPosition implements Comparable<IteratorPosition>, Predicate<Record> {

        private final String streamArn;
        private final String shardId;
        private final BigInteger sequenceNumber;

        IteratorPosition(String streamArn, String shardId, BigInteger sequenceNumber) {
            this.streamArn = checkNotNull(streamArn);
            this.shardId = checkNotNull(shardId);
            this.sequenceNumber = checkNotNull(sequenceNumber);
        }

        /**
         * Implementation of Predicate interface so this position can be used directly to filter records. Also enables
         * negating the precedes check if this position succeeds records.
         */
        @Override
        public boolean test(Record record) {
            return precedes(record);
        }

        /**
         * Checks if this iterator is before or at the given record position in the shard. Note: the client is
         * responsible for making sure the record is from the same stream and shard; records do not carry shard
         * information, so the implementation cannot verify they match. The behavior of comparing iterator positions and
         * records from different shards in unspecified.
         *
         * @param record Record to check.
         * @return true if the sequence number of the record is greater or equal to the position sequence number.
         */
        boolean precedes(Record record) {
            return sequenceNumber.compareTo(parseSequenceNumber(record)) <= 0;
        }

        /**
         * Returns true if this position precedes any records in the given result. Since DynamoDB orders records by
         * sequence number that equivalent to checking whether this position precedes the last record.
         *
         * @param result Result to check. Records list must not be empty.
         * @return True if this position precedes any records in the given result, false otherwise.
         */
        boolean precedesAny(GetRecordsResult result) {
            assert !result.getRecords().isEmpty();
            return precedes(getLast(result.getRecords()));
        }

        /**
         * Returns the position immediately following this one, i.e., the next sequence number in the same stream
         * shard.
         *
         * @return Next iterator position.
         */
        IteratorPosition next() {
            return new IteratorPosition(streamArn, shardId, sequenceNumber.add(ONE));
        }

        /**
         * Returns the position immediately following the last record in the given result.
         *
         * @param result Result to position after. Record list must not be empty.
         * @return IteratorPosition immediately following the last record sequence number in the result.
         */
        IteratorPosition next(GetRecordsResult result) {
            assert !result.getRecords().isEmpty();
            return new IteratorPosition(streamArn, shardId, parseSequenceNumber(getLast(result.getRecords()))).next();
        }

        /**
         * Returns a shard iterator that starts after the last record in the given result.
         *
         * @param result Result to position the iterator after. Record list must not be empty.
         * @param dynamoDbIterator Optional DynamoDB-level iterator after the last record.
         * @return ShardIterator that is positioned after the given result.
         */
        ShardIterator nextShardIterator(GetRecordsResult result, @Nullable String dynamoDbIterator) {
            assert !result.getRecords().isEmpty();
            return new ShardIterator(streamArn, shardId, AFTER_SEQUENCE_NUMBER,
                getLast(result.getRecords()).getDynamodb().getSequenceNumber(), dynamoDbIterator);
        }

        /**
         * Compares the given position to this position. Sorts positions in different stream shards lexicographically by
         * their streamArn and shardId. Within a given stream shard, positions are sorted numerically by their sequence
         * number.
         *
         * @param o Other position.
         * @return negative integer, zero, or positive integer as this position is less than, equal, or greater other.
         */
        @Override
        public int compareTo(@Nonnull IteratorPosition o) {
            int c = streamArn.compareTo(o.streamArn);
            if (c != 0) {
                return c;
            }
            c = shardId.compareTo(o.shardId);
            if (c != 0) {
                return c;
            }
            return sequenceNumber.compareTo(o.sequenceNumber);
        }

        /**
         * Checks whether the given iterator position has the same streamArn and shardId.
         *
         * @param o Other iterator position.
         * @return true if the other iterator position has the same same streamArn and shardId, false otherwise.
         */
        boolean equalsShard(IteratorPosition o) {
            return streamArn.equals(o.streamArn) && shardId.equals(o.shardId);
        }

        /**
         * Checks given object for equality.
         *
         * @param o Other object.
         * @return True if the given object is an IteratorPosition with the same streamArn, shardId, and sequenceNumber.
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
            return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(sequenceNumber, that.sequenceNumber);
        }

        /**
         * Hash code of this iterator position including streamArn, shardId, and sequence number.
         *
         * @return Hash code.
         */
        @Override
        public int hashCode() {
            return Objects.hash(streamArn, shardId, sequenceNumber);
        }

    }


    /**
     * A logical shard iterator that optionally wraps an underlying DynamoDB iterator.
     */
    private static final class ShardIterator {

        private static final CompositeStrings compositeStrings = new CompositeStrings('/', '\\');

        /**
         * Returns an iterator for the given request and optional DynamoDB iterator.
         *
         * @param request Iterator request.
         * @param dynamoDbIterator DynamoDB iterator (optional).
         * @return Logical shard iterator.
         */
        static ShardIterator fromRequest(GetShardIteratorRequest request, @Nullable String dynamoDbIterator) {
            return new ShardIterator(
                request.getStreamArn(),
                request.getShardId(),
                ShardIteratorType.fromValue(request.getShardIteratorType()),
                request.getSequenceNumber(),
                dynamoDbIterator
            );
        }

        /**
         * Parses a ShardIterator instance from its external String representation.
         *
         * @param value External string form.
         * @return ShardIterator instance.
         */
        static ShardIterator fromExternalString(String value) {
            Iterator<String> it = compositeStrings.split(value);
            String streamArn = it.next();
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
            String dynamoDbIterator = it.hasNext() ? it.next() : null;
            return new ShardIterator(streamArn, shardId, type, sequenceNumber, dynamoDbIterator);
        }

        private static String getSequenceNumber(String pointer, ShardIteratorType type) {
            int length = type.toString().length();
            checkArgument(pointer.length() > length + 2);
            checkArgument(pointer.charAt(length) == '.');
            return pointer.substring(length + 1);
        }

        @Nonnull
        private final String streamArn;
        @Nonnull
        private final String shardId;
        @Nonnull
        private final ShardIteratorType type;
        @Nullable
        private final String sequenceNumber;
        @Nullable
        private final BigInteger parsedSequenceNumber;
        @Nullable
        private String dynamoDbIterator;

        private ShardIterator(
            @Nonnull String streamArn,
            @Nonnull String shardId,
            @Nonnull ShardIteratorType type,
            @Nullable String sequenceNumber,
            @Nullable String dynamoDbIterator) {
            this.streamArn = checkNotNull(streamArn);
            this.shardId = checkNotNull(shardId);
            this.type = type;
            this.dynamoDbIterator = dynamoDbIterator;

            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    checkArgument(sequenceNumber == null);
                    this.sequenceNumber = null;
                    this.parsedSequenceNumber = null;
                    break;
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    checkArgument(sequenceNumber != null);
                    this.sequenceNumber = sequenceNumber;
                    this.parsedSequenceNumber = parseSequenceNumber(sequenceNumber);
                    break;
                default:
                    throw new RuntimeException("Missing case statement for ShardIteratorType");
            }
        }

        /**
         * Returns a new iterator with the given dynamoDbIterator.
         *
         * @param dynamoDbIterator DynamoDb iterator.
         * @return New iterator.
         */
        ShardIterator withDynamoDbIterator(String dynamoDbIterator) {
            this.dynamoDbIterator = dynamoDbIterator;
            return this;
        }

        /**
         * Returns underlying streams iterator, loading it if present.
         *
         * @param streams Streams instance to use for loading iterator if needed.
         * @return DynamoDB iterator.
         */
        String getDynamoDbIterator(AmazonDynamoDBStreams streams) {
            if (dynamoDbIterator == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading DynamoDB iterator: iterator={}", this);
                }
                dynamoDbIterator = streams.getShardIterator(toRequest()).getShardIterator();
            }
            return dynamoDbIterator;
        }

        /**
         * Iterators that point to a specific sequence number (<code>AT_SEQUENCE_NUMBER</code> and
         * <code>AFTER_SEQUENCE_NUMBER</code>) can be resolved directly to a position in the stream shard without
         * additional context. We refer to these types of iterators <i>immutable</i> or <i>absolute</i>, since their
         * position in the stream shard does not change as records are added to or removed from the shard. Iterators
         * that refer to logical positions in the shard (<code>TRIM_HORIZON</code> and <code>LATEST</code>) cannot be
         * resolved directly to a position, since their position changes based on the records in the underlying stream
         * shard.
         *
         * @return position for immutable iterators, empty otherwise
         */
        Optional<IteratorPosition> resolvePosition() {
            switch (type) {
                case TRIM_HORIZON:
                case LATEST:
                    return Optional.empty();
                case AT_SEQUENCE_NUMBER:
                    return Optional.of(new IteratorPosition(streamArn, shardId, parsedSequenceNumber));
                case AFTER_SEQUENCE_NUMBER:
                    return Optional.of(new IteratorPosition(streamArn, shardId, parsedSequenceNumber.add(ONE)));
                default:
                    throw new RuntimeException("Unhandled switch case");
            }
        }

        /**
         * Resolves the position of this iterator relative to the first record returned by a query using this iterator.
         * If the iterator is absolute, its position is returned. Otherwise the position of the first record in the
         * shard is returned.
         *
         * @param records Non-empty records list loaded for this iterator.
         * @return Resolved iterator position.
         */
        IteratorPosition resolvePosition(List<Record> records) {
            assert !records.isEmpty();
            return resolvePosition()
                .orElseGet(() -> new IteratorPosition(streamArn, shardId, parseSequenceNumber(records.get(0))));
        }

        /**
         * Returns a new virtual shard iterator that starts at the sequence number immediately after the last record in
         * the given records list.
         *
         * @param records Non-empty records list.
         * @return New shard iterator that starts after the last record in the list.
         */
        ShardIterator nextShardIterator(List<Record> records) {
            assert !records.isEmpty();
            return new ShardIterator(streamArn, shardId, AFTER_SEQUENCE_NUMBER,
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
         * Serializes this ShardIterator into its external string format.
         *
         * @return Externalized string.
         */
        String toExternalString() {
            List<String> fields = new ArrayList<>(4);
            fields.add(streamArn);
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
            if (dynamoDbIterator != null) {
                fields.add(dynamoDbIterator);
            }
            return compositeStrings.join(fields);
        }

        @Override
        public String toString() {
            return toExternalString();
        }
    }

    // logger instance
    private static final Logger LOG = LoggerFactory.getLogger(CachingAmazonDynamoDbStreams.class);

    // DynamoDB's GetRecords limit. Constant defined in AmazonDynamoDBStreamsAdapterClient (but not on classpath).
    static final int GET_RECORDS_LIMIT = 1000;

    /**
     * Parses the given string representation of a DynamoDB sequence number into a BigInteger.
     *
     * @param sequenceNumber String representation of sequence number.
     * @return BigInteger value.
     */
    private static BigInteger parseSequenceNumber(String sequenceNumber) {
        try {
            return new BigInteger(sequenceNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Parses the sequence number of the given record into a BigInteger.
     *
     * @param record Record with sequence number.
     * @return BigInteger sequence number value.
     */
    private static BigInteger parseSequenceNumber(Record record) {
        return parseSequenceNumber(record.getDynamodb().getSequenceNumber());
    }

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

    // cache for quasi-immutable values that maintains insertion order for LRU removal
    private final Map<IteratorPosition, GetRecordsResult> recordsCache;
    // index on position for efficient position-based cache lookups
    private final NavigableMap<IteratorPosition, GetRecordsResult> recordsCacheIndex;
    // looks for mutating lock
    private final ReadWriteLock recordsCacheLock;
    // size of cache >= 0
    private long recordsCacheByteSize;

    private CachingAmazonDynamoDbStreams(AmazonDynamoDBStreams amazonDynamoDbStreams,
        Sleeper sleeper,
        long maxRecordsByteSize,
        int maxGetRecordsRetries,
        long getRecordsLimitExceededBackoffInMillis) {
        super(amazonDynamoDbStreams);
        this.sleeper = sleeper;
        this.maxGetRecordsRetries = maxGetRecordsRetries;
        this.getRecordsLimitExceededBackoffInMillis = getRecordsLimitExceededBackoffInMillis;

        this.recordsCache = new LinkedHashMap<IteratorPosition, GetRecordsResult>() {
            @Override
            protected boolean removeEldestEntry(Entry<IteratorPosition, GetRecordsResult> eldest) {
                if (recordsCacheByteSize > maxRecordsByteSize) {
                    removedCacheEntry(eldest);
                    return true;
                }
                return false;
            }
        };
        this.recordsCacheIndex = new TreeMap<>();
        this.recordsCacheByteSize = 0L;
        this.recordsCacheLock = new ReentrantReadWriteLock();
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
                // TODO add 15 min timeout to lazy iterators?
                dynamoDbIterator = null;
                break;
            default:
                throw new RuntimeException("Missing switch case on ShardIteratorType");
        }

        ShardIterator iterator = ShardIterator.fromRequest(request, dynamoDbIterator);
        return new GetShardIteratorResult().withShardIterator(iterator.toExternalString());
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRecords request={}", request);
        }

        // parse iterator
        final ShardIterator iterator = ShardIterator.fromExternalString(request.getShardIterator());

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
            // if iterator is resolvable, try to lookup records in cache
            Optional<GetRecordsResult> cached = iterator.resolvePosition().flatMap(this::getFromCache);
            if (cached.isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getRecords cache hit: iterator={}, result={}", iterator, toShortString(cached.get()));
                }
                return cached.get();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getRecords cache miss: iterator={}", iterator);
                }
            }

            // If we have a cache miss, get DynamoDB iterator (load if needed)
            final String shardIterator = iterator.getDynamoDbIterator(dynamoDbStreams);

            // next load records from stream
            final GetRecordsResult loadedRecordsResult;
            try {
                loadedRecordsResult = dynamoDbStreams.getRecords(
                    new GetRecordsRequest().withShardIterator(shardIterator));
            } catch (LimitExceededException e) {
                long backoff = (getRecordsRetries + 1) * getRecordsLimitExceededBackoffInMillis;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("getRecords limit exceeded: iterator={}, retry attempt={}, backoff={}.", iterator,
                        getRecordsRetries, backoff);
                }
                sleeper.sleep(backoff);
                getRecordsRetries++;
                continue;
            } // TODO could catch ExpiredIteratorException and automatically renew shard iterators in cached results

            if (LOG.isDebugEnabled()) {
                LOG.debug("getRecords loaded records: iterator={}, result={}", iterator,
                    toShortString(loadedRecordsResult));
            }

            // if we didn't load anything, return without adding cache segment (preserves non-empty range invariant)
            // TODO could cache empty results for (short) time period to avoid having every client hit the stream.
            if (loadedRecordsResult.getRecords().isEmpty()) {
                if (loadedRecordsResult.getNextShardIterator() == null) {
                    return loadedRecordsResult;
                }
                // replace with loaded iterator, so it is used to proceed through stream on next call
                return new GetRecordsResult()
                    .withRecords(loadedRecordsResult.getRecords())
                    .withNextShardIterator(
                        iterator.withDynamoDbIterator(loadedRecordsResult.getNextShardIterator()).toExternalString());
            }

            // otherwise (if we found records), try to update the cache
            Optional<GetRecordsResult> cachedResult;
            GetRecordsResult result;
            final Lock writeLock = recordsCacheLock.writeLock();
            writeLock.lock();
            try {
                // Resolve iterator position (either to sequence number it specifies or first record sequence number)
                IteratorPosition loadedPosition = iterator.resolvePosition(loadedRecordsResult.getRecords());

                // Add retrieved records to cache under that position
                cachedResult = addToCache(loadedPosition, loadedRecordsResult);

                // now lookup result: may not be exactly what we loaded if we merged result with other segments.
                result = getFromCache(loadedPosition).get();
            } finally {
                writeLock.unlock();
            }

            // log cache  outside of critical section
            if (LOG.isDebugEnabled()) {
                LOG.debug("getRecords cached result={}", cachedResult);
            }

            return result;
        }

        if (LOG.isWarnEnabled()) {
            LOG.warn("GetRecords exceeded maximum number of retries");
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
                .withNextShardIterator(iterator.nextShardIterator(records).toExternalString());
        }
        return result;
    }

    /**
     * Looks up cached result for given position. Acquires read lock to access cache, but may be called with read or
     * write lock held, since lock is reentrant.
     *
     * @param position Iterator for which to retrieve matching records from the cache
     * @return List of matching (i.e., immediately succeeding iterator) cached records or empty list if none match
     */
    private Optional<GetRecordsResult> getFromCache(IteratorPosition position) {
        final Lock readLock = recordsCacheLock.readLock();
        readLock.lock();
        try {
            final Optional<GetRecordsResult> cachedRecordsResult;
            final Map.Entry<IteratorPosition, GetRecordsResult> previousCacheEntry = getFloorCacheEntry(position);
            if (previousCacheEntry == null) {
                // no matching cache entry found
                cachedRecordsResult = Optional.empty();
            } else {
                IteratorPosition previousPosition = previousCacheEntry.getKey();
                GetRecordsResult previousResult = previousCacheEntry.getValue();
                if (position.equals(previousPosition)) {
                    // exact iterator hit (hopefully common case), return all cached records
                    cachedRecordsResult = Optional.of(previousResult);
                } else if (position.equalsShard(previousPosition) && position.precedesAny(previousResult)) {
                    // Cache entry contains records that match (i.e., come after) the requested iterator
                    // position: Filter cached records to those that match. Return only that subset, to increase
                    // the chance of using a shared iterator position on the next getRecords call.
                    final List<Record> matchingCachedRecords = previousResult.getRecords().stream()
                        .filter(position)
                        .collect(toList());
                    return Optional.of(new GetRecordsResult()
                        .withRecords(matchingCachedRecords)
                        .withNextShardIterator(previousResult.getNextShardIterator()));
                } else {
                    // no cached records in the preceding cache entry match the requested position (i.e., all records
                    // precede it)
                    cachedRecordsResult = Optional.empty();
                }
            }
            return cachedRecordsResult;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Adds the given loaded result into the cache under the given loaded position. Discards records that overlap with
     * existing cache entries. If the entry is adjacent to existing entries, it will merge them, provided the resulting
     * record list does not exceed {@link #GET_RECORDS_LIMIT}. Returns the result that was actually added to the cache
     * (which may include merged records). The loaded position may precede the first record, since seqeuence numbers are
     * not contiguous.
     *
     * @param loadedPosition Position from which the result was loaded in the stream.
     * @param loadedResult Result loaded for the given position.
     * @return Result actually added to cache. Empty if all records were already present in cache for position.
     */
    private Optional<GetRecordsResult> addToCache(IteratorPosition loadedPosition, GetRecordsResult loadedResult) {
        final Lock writeLock = recordsCacheLock.writeLock();
        writeLock.lock();
        try {
            IteratorPosition cachePosition = loadedPosition;
            GetRecordsResult cacheResult = new GetRecordsResult().withRecords(loadedResult.getRecords());
            Optional.ofNullable(loadedResult.getNextShardIterator())
                .map(nextIterator -> loadedPosition.nextShardIterator(loadedResult, nextIterator).toExternalString())
                .ifPresent(cacheResult::setNextShardIterator);

            boolean predecessorAdjacent = false;
            final Entry<IteratorPosition, GetRecordsResult> predecessor = getFloorCacheEntry(loadedPosition);
            if (predecessor != null && loadedPosition.equalsShard(predecessor.getKey())) {
                GetRecordsResult predecessorResult = predecessor.getValue();
                if (loadedPosition.precedesAny(predecessorResult)) {
                    // the previous cache entry overlaps with the records we retrieved: filter out overlapping records
                    // (by reducing the loaded records to those that come after the last predecessor record)
                    cachePosition = loadedPosition.next(predecessorResult);
                    cacheResult.setRecords(cacheResult.getRecords().stream()
                        .filter(cachePosition)
                        .collect(toList()));
                    // if all retrieved records are contained in the predecessor, we have nothing to add
                    if (cacheResult.getRecords().isEmpty()) {
                        return Optional.empty();
                    }
                    predecessorAdjacent = true;
                } else {
                    //
                    predecessorAdjacent = loadedPosition.equals(loadedPosition.next(predecessorResult));
                }
            }

            boolean successorAdjacent = false;
            final Entry<IteratorPosition, GetRecordsResult> successor = getHigherCacheEntry(cachePosition);
            if (successor != null && cachePosition.equalsShard(successor.getKey())) {
                IteratorPosition successorPosition = successor.getKey();
                if (successorPosition.precedesAny(cacheResult)) {
                    // the succeeding cache entry overlaps with loaded records: filter out overlapping records
                    // (by reducing the loaded records to those that come before the successor starting position)
                    cacheResult.setRecords(cacheResult.getRecords().stream()
                        .filter(successorPosition.negate())
                        .collect(toList()));

                    if (cacheResult.getRecords().isEmpty()) {
                        // if all retrieved records are contained in the successor, reindex (and maybe merge) successor
                        removeCacheEntry(successor);
                        cacheResult = successor.getValue();
                        successorAdjacent = false;
                    } else {
                        // if some of the retrieved records are not contained in the next segment,
                        cacheResult.setNextShardIterator(
                            cachePosition.nextShardIterator(cacheResult, null).toExternalString());
                        successorAdjacent = true;
                    }
                } else {
                    successorAdjacent = successorPosition.equals(cachePosition.next(cacheResult));
                }
            }

            if (predecessorAdjacent) {
                int totalSize = predecessor.getValue().getRecords().size() + cacheResult.getRecords().size();
                if (totalSize <= GET_RECORDS_LIMIT) {
                    List<Record> mergedRecords = new ArrayList<>(totalSize);
                    mergedRecords.addAll(predecessor.getValue().getRecords());
                    mergedRecords.addAll(cacheResult.getRecords());
                    cacheResult.setRecords(mergedRecords);
                    cachePosition = predecessor.getKey();
                    removeCacheEntry(predecessor);
                }
            }
            if (successorAdjacent) {
                int totalSize = cacheResult.getRecords().size() + successor.getValue().getRecords().size();
                if (totalSize <= GET_RECORDS_LIMIT) {
                    List<Record> mergedRecords = new ArrayList<>(totalSize);
                    mergedRecords.addAll(cacheResult.getRecords());
                    mergedRecords.addAll(successor.getValue().getRecords());
                    cacheResult.setRecords(mergedRecords);
                    cacheResult.setNextShardIterator(successor.getValue().getNextShardIterator());
                    removeCacheEntry(successor);
                }
            }

            addCacheEntry(cachePosition, cacheResult);

            return Optional.of(cacheResult);
        } finally {
            writeLock.unlock();
        }
    }

    private void addCacheEntry(IteratorPosition key, GetRecordsResult value) {
        GetRecordsResult previous = recordsCache.put(key, value);
        assert previous == null;
        previous = recordsCacheIndex.put(key, value);
        assert previous == null;
        recordsCacheByteSize += getByteSize(value);
    }

    private void removeCacheEntry(Entry<IteratorPosition, GetRecordsResult> entry) {
        GetRecordsResult value = recordsCache.remove(entry.getKey());
        assert value == entry.getValue();
        removedCacheEntry(entry);
    }

    private void removedCacheEntry(Entry<IteratorPosition, GetRecordsResult> entry) {
        GetRecordsResult value = recordsCacheIndex.remove(entry.getKey());
        assert value == entry.getValue();
        recordsCacheByteSize -= getByteSize(entry.getValue());
        assert recordsCacheByteSize >= 0;
    }

    private long getByteSize(GetRecordsResult value) {
        return value.getRecords().stream().map(Record::getDynamodb).mapToLong(StreamRecord::getSizeBytes).sum();
    }

    private Entry<IteratorPosition, GetRecordsResult> getFloorCacheEntry(IteratorPosition position) {
        return recordsCacheIndex.floorEntry(position);
    }

    private Entry<IteratorPosition, GetRecordsResult> getHigherCacheEntry(IteratorPosition position) {
        return recordsCacheIndex.higherEntry(position);
    }

}
