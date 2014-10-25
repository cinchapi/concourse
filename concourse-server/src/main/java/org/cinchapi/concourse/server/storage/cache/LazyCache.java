/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.cache;

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
 * @author jnelson
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
     * @author jnelson
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
