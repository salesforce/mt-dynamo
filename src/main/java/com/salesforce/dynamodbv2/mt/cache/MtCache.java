/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class MtCache<V> implements Cache<Object, V> {

    private static class Key {
        private final String context;
        private final Object key;

        Key(@Nullable String context, Object key) {
            this.context = context;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key1 = (Key) o;
            return Objects.equals(context, key1.context)
                && Objects.equals(key, key1.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, key);
        }
    }

    private final MtAmazonDynamoDbContextProvider contextProvider;
    private final Cache<Object, V> cache;

    public MtCache(MtAmazonDynamoDbContextProvider contextProvider, Cache<Object, V> cache) {
        this.contextProvider = contextProvider;
        this.cache = cache;
    }

    private Key getKey(Object key) {
        return new Key(contextProvider.getContextOpt().orElse(null), key);
    }

    @Override
    public V getIfPresent(@Nullable Object key) {
        return cache.getIfPresent(getKey(key));
    }

    @Override
    public V get(@Nullable Object key,
                 @Nullable Callable<? extends V> valueLoader) throws ExecutionException {
        return cache.get(getKey(key), Objects.requireNonNull(valueLoader));
    }

    @Override
    public void put(@Nullable Object key,
                    @Nullable V value) {
        cache.put(getKey(key), Objects.requireNonNull(value));
    }

    @Override
    public void putAll(Map<?, ? extends V> m) {
        m.forEach(this::put);
    }

    @Override
    public void invalidate(@Nullable Object key) {
        cache.invalidate(getKey(key));
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        cache.invalidateAll(StreamSupport.stream(keys.spliterator(), false)
            .map(this::getKey).collect(Collectors.toList()));
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public ConcurrentMap<Object, V> asMap() {
        return cache.asMap();
    }

    @Override
    public ImmutableMap<Object, V> getAllPresent(@Nullable Iterable<?> keys) {
        throw new UnsupportedOperationException();
    }

}
