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
package com.cinchapi.concourse.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.cinchapi.concourse.annotate.Experimental;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

/**
 * An AutoMap automatically and atomically manages entry creation and removal
 * using provided {@code loader} and {@code cleaner} functions. This is
 * especially useful if you have a map that associates a key to another
 * collection and you want to be able to access the mapped collection without
 * first checking if it exist and you want empty collections to be removed
 * nearly on the fly without explicitly checking.
 * <p>
 * 
 * Using an AutoMap you can replace
 * 
 * <pre>
 * V value = map.get(key);
 * if(value == null) {
 *     value = load(value);
 *     map.put(key, value);
 * }
 * </pre>
 * 
 * with
 * 
 * <pre>
 * V value = map.get(key);
 * </pre>
 * 
 * and be sure that {@code value} is not {@code null} because it has been
 * previously set or atomically loaded and set with the provided {@code loader}
 * function.
 * </p>
 * <p>
 * Additionally, you never have to worry about removing entries from the map or
 * doing any kind of cleanup based on value conditions similar to
 * 
 * <pre>
 * V value = map.get(key);
 * modify(value);
 * if(satisifiesCondition(value)) {
 *     map.remove(key);
 * }
 * </pre>
 * 
 * because a background thread periodically goes through the map and atomically
 * cleans up eligible entries. The background thread uses local synchronization
 * on entries so it never interferes with overall map operations.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Experimental
public abstract class AutoMap<K, V> extends AbstractMap<K, V> {

    /**
     * Return an {@link AutoMap} that is a drop in replacement for a
     * {@link HashMap}.
     * 
     * @param loader
     * @param cleaner
     * @return the AutoMap
     */
    public static <K, V> AutoHashMap<K, V> newAutoHashMap(
            Function<K, V> loader, Function<V, Boolean> cleaner) {
        return new AutoHashMap<K, V>(loader, cleaner);
    }

    /**
     * Return an {@link AutoMap} that is a drop in replacement for a
     * {@link TreeMap}.
     * 
     * @param loader
     * @param cleaner
     * @return the AutoMap
     */
    public static <K extends Comparable<K>, V> AutoSkipListMap<K, V> newAutoSkipListMap(
            Function<K, V> loader, Function<V, Boolean> cleaner) {
        return new AutoSkipListMap<K, V>(loader, cleaner);
    }

    /**
     * Set the amount of time between each cleanup run. Changing these values
     * won't affect existing AutoMap instances, but subsequent creations will
     * reflect the updated values.
     * 
     * @param delay
     * @param unit
     */
    // --- visible for testing
    protected static void setCleanupDelay(long delay, TimeUnit unit) {
        CLEANUP_DELAY = delay;
        CLEANUP_DELAY_UNIT = unit;
    }

    /**
     * This method is provided to access {@link #CLEANUP_DELAY} because the
     * value may change during testing if
     * {@link #setCleanupDelay(long, TimeUnit)}. is called.
     * 
     * @return {@link #CLEANUP_DELAY}.
     */
    private static long getCleanupDelay() {
        return CLEANUP_DELAY;
    }

    /**
     * This method is provided to access {@link #CLEANUP_DELAY_UNIT} because the
     * value may change during testing if
     * {@link #setCleanupDelay(long, TimeUnit)}. is called.
     * 
     * @return {@link #CLEANUP_DELAY_UNIT}.
     */
    private static TimeUnit getCleanupDelayUnit() {
        return CLEANUP_DELAY_UNIT;
    }

    /**
     * The duration of time measured in {@link #CLEANUP_DELAY_UNIT} that the
     * {@link #cleanupThread} will sleep between cleans.
     */
    private static long CLEANUP_DELAY = 60;

    /**
     * The time unit for {@link #CLEANUP_DELAY}.
     */
    private static TimeUnit CLEANUP_DELAY_UNIT = TimeUnit.SECONDS;

    /**
     * The map that serves as the backingStore for the data managed by this
     * class.
     */
    protected final Map<K, V> backingStore;

    /**
     * The caller defined loader function.
     */
    private final Function<K, V> loader;

    /**
     * The caller defined cleaner function.
     */
    private final Function<V, Boolean> cleaner;

    /**
     * A background thread that iterates through the map and cleans up eligible
     * entries from time to time.
     */
    private final Thread cleanupThread = new Thread() {
        {
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                for (Entry<K, V> entry : backingStore.entrySet()) {
                    synchronized (entry) {
                        if(cleaner.apply(entry.getValue())) {
                            backingStore.remove(entry.getKey());
                        }
                    }
                }
                try {
                    getCleanupDelayUnit().sleep(getCleanupDelay());
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

    };

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     * @param loader
     * @param cleaner
     */
    protected AutoMap(Map<K, V> backingStore, Function<K, V> loader,
            Function<V, Boolean> cleaner) {
        this.backingStore = backingStore;
        this.loader = loader;
        this.cleaner = cleaner;
        cleanupThread.start();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return backingStore.entrySet();
    }

    /**
     * Return the value mapped from {@code key} if it exists, otherwise, load
     * the value using the provided loader function.
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        synchronized (key) {
            V value = backingStore.get(key);
            if(value == null) {
                value = loader.apply((K) key);
                backingStore.put((K) key, value);
            }
            return value;
        }
    }
}
