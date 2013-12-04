/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.annotate.Experimental;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

/**
 * An AutoMap automatically manages entry creation and removal using provided
 * {@code loader} and {@code cleaner} functions.
 * 
 * @author jnelson
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
