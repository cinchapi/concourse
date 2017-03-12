/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage.cache;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link LazyCache} is a simple and eventually consistent cache:
 * {@link #put(Object, Object) Put} items into the collection and
 * eventually a call to {@link #get(Object)} will reflect the update.
 * <p>
 * The primary purpose of a {@link LazyCache} is to accommodate situations where
 * caching helps performance, but retrieving cached data is not critical for
 * correctness (i.e. worst case having duplicate instances of data won't lead to
 * bad results). In particular this is designed to avoid overhead with placing
 * new items into the cache when there is a cache miss.
 * </p>
 * <p>
 * When placing items into the cache, the method returns immediately and the
 * cache addition is scheduled to run in the background at some later time. So
 * this cache only guarantees that it will eventually make updates and calls to
 * retrieve items may report stale data for some time. Therefore, this cache is
 * not a good candidate for data that changes frequently. It's best used as a
 * sort of intern pool for immutable objects.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class LazyCache<K, V> {

    /**
     * Return a {@link LazyCache} that is initially sized to accommodate the
     * specified number of elements.
     * 
     * @param size
     * @return the LazyCache
     */
    public static <K, V> LazyCache<K, V> withExpectedSize(int size) {
        return new LazyCache<K, V>(Maps.<K, V> newHashMapWithExpectedSize(size));
    }

    /**
     * The service that handles placing items into the cache.
     */
    private final ExecutorService executor;

    /**
     * The internal data structure the holds the cached content.
     */
    private final Map<K, V> internal;

    /**
     * Construct a new instance
     * 
     * @param initialSize
     */
    private LazyCache(Map<K, V> internal) {
        this.internal = internal;
        executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("lazy-cache-" + System.identityHashCode(this))
                .build());
    }

    /**
     * Return the value, if any, that is associated with the {@code key} in this
     * cache. Recent updates to the cache are not guaranteed to be reflected.
     * 
     * @param key
     * @return the value associated with {@code key} at the time of invocation,
     *         or {@code null} if no such value exists
     */
    @Nullable
    public V get(K key) {
        return internal.get(key);
    }

    /**
     * Associate {@code key} with {@code value} in the cache. This update will
     * eventually take effect.
     * 
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        executor.execute(new PutRunnable(key, value));
    }

    /**
     * The {@link Runnable} that is passed to the {@link #executor} to add items
     * to the cache.
     * 
     * @author Jeff Nelson
     */
    private final class PutRunnable implements Runnable {

        private final K key;
        private final V value;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param value
         */
        public PutRunnable(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            internal.put(key, value);
        }

    }

}
