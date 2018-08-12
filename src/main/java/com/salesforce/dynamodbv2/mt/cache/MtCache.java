/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class MtCache<V> implements Cache<String, V> {

    private static final String DELIMITER = "-";
    private final MtAmazonDynamoDbContextProvider contextProvider;
    private final Cache<Object, V> cache;

    public MtCache(MtAmazonDynamoDbContextProvider contextProvider) {
        this.contextProvider = contextProvider;
        this.cache = CacheBuilder.newBuilder().build();
    }

    private String getKey(Object key) {
        return contextProvider.getContext() + DELIMITER + key;
    }

    @Override
    public V getIfPresent(Object key) {
        return cache.getIfPresent(getKey(key));
    }

    @Override
    public V get(String key, Callable<? extends V> valueLoader) throws ExecutionException {
        return cache.get(getKey(key), valueLoader);
    }

    @Override
    public void put(String key, V value) {
        cache.put(getKey(key), value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        m.forEach(this::put);
    }

    @Override
    public void invalidate(Object key) {
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
    public ConcurrentMap asMap() {
        return cache.asMap();
    }

    @Override
    public ImmutableMap<String, V> getAllPresent(Iterable<?> keys) {
        throw new UnsupportedOperationException();
    }

}
