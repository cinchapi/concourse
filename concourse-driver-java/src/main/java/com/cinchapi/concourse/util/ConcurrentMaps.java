/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Throwables;

/**
 * Utility class for the {@link ConcurrentMap} interface.
 * 
 * @author Jeff Nelson
 */
public final class ConcurrentMaps {

    /**
     * Place the {@code key} in the {@code map} and associated it with the
     * {@code value}. Finally, send a signal to threads waiting on
     * {@link #waitAndRemove(ConcurrentMap, Object) a signal to remove} the
     * {@code key}.
     * 
     * @param map the {@link ConcurrentMap map} into which the {@code key} and
     *            {@code value} are placed
     * @param key the key to associate with the {@code value}
     * @param value the value to associate with the {@code key}
     * @return the value that was previously associated with {@code key} in the
     *         {@code map}
     */
    public static <K, V> V putAndSignal(ConcurrentMap<K, V> map, K key, V value) {
        V ret = map.put(key, value);
        String hashCode = String.valueOf(key.hashCode()).intern();
        synchronized (hashCode) {
            hashCode.notifyAll();
        }
        return ret;
    }

    /**
     * Execute the {@link ConcurrentMap#remove(Object)} method while causing the
     * current thread to block, if necessary, until the {@code key} is in the
     * map and associated with a {@code non-null} value.
     * 
     * <p>
     * This method should only be used in conjunction with the
     * {@link #putAndSignal(ConcurrentMap, Object, Object)} method.
     * </p>
     * 
     * @param map the {@link ConcurrentMap map} from which the {@code key} is
     *            removed
     * @param key the key to remove from the map
     * @return the {@code non-null} value that was associated with a {@code key}
     *         and is therefore removed.
     */
    public static <K, V> V waitAndRemove(ConcurrentMap<K, V> map, K key) {
        long start = System.currentTimeMillis();
        V value = null;
        while ((value = map.remove(key)) == null) {
            if(System.currentTimeMillis() - start < SPIN_THRESHOLD_IN_MILLIS) {
                continue;
            }
            else {
                String hashCode = String.valueOf(key.hashCode()).intern();
                synchronized (hashCode) {
                    if((value = map.remove(key)) == null) {
                        try {
                            hashCode.wait();
                        }
                        catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    else {
                        break;
                    }
                }
            }
        }
        return value;
    }

    /**
     * The amount of time to spin before backing off and waiting.
     */
    protected static int SPIN_THRESHOLD_IN_MILLIS = 1000; // visible for
                                                          // testing

}
