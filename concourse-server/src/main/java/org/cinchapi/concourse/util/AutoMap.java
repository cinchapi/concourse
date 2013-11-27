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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.annotate.Experimental;

import com.google.common.base.Function;

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
    protected static void setCleanupDelay(long delay, TimeUnit unit) { // visible
                                                                       // for
                                                                       // testing
        CLEANUP_DELAY = delay;
        CLEANUP_DELAY_UNIT = unit;
    }

    private static long CLEANUP_DELAY = 60;
    private static TimeUnit CLEANUP_DELAY_UNIT = TimeUnit.SECONDS;

    protected final Map<K, V> backingStore;
    private final Function<K, V> loader;
    private final Function<V, Boolean> cleaner;
    private final ScheduledExecutorService scheduler = Executors
            .newScheduledThreadPool(1, new ThreadFactory() {

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }

            });

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
        scheduler.scheduleWithFixedDelay(clean(), 0, CLEANUP_DELAY,
                CLEANUP_DELAY_UNIT);
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
        V value = backingStore.get(key);
        if(value == null) {
            value = loader.apply((K) key);
            backingStore.put((K) key, value);
        }
        return value;
    }

    /**
     * Return a Runnable command that will go through {@link #backingStore} and
     * apply {@link #cleaner} to all the container values.
     * 
     * @return the Runnable command
     */
    private Runnable clean() {
        return new Runnable() {

            @Override
            public void run() {
                synchronized (backingStore) {
                    for (Entry<K, V> entry : backingStore.entrySet()) {
                        if(cleaner.apply(entry.getValue())) {
                            backingStore.remove(entry.getKey());
                        }
                    }
                }
            }

        };
    }

}
