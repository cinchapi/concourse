/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package jsr166e;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import jsr166e.ConcurrentHashMapV8;
import jsr166e.LongAdder;

/**
 * A keyed table of adders, that may be useful in computing frequency
 * counts and histograms, or may be used as a form of multiset. A
 * {@link LongAdder} is associated with each key. Keys are added to
 * the table implicitly upon any attempt to update, or may be added
 * explicitly using method {@link #install}.
 * 
 * <p>
 * <em>jsr166e note: This class is targeted to be placed in
 * java.util.concurrent.atomic.</em>
 * 
 * @since 1.8
 * @author Doug Lea
 */
public class LongAdderTable<K> implements Serializable {
    /** Relies on default serialization */
    private static final long serialVersionUID = 7249369246863182397L;

    /** The underlying map */
    private final ConcurrentHashMapV8<K, LongAdder> map;

    static final class CreateAdder
            implements ConcurrentHashMapV8.Fun<Object, LongAdder> {
        public LongAdder apply(Object unused) {
            return new LongAdder();
        }
    }

    private static final CreateAdder createAdder = new CreateAdder();

    /**
     * Creates a new empty table.
     */
    public LongAdderTable() {
        map = new ConcurrentHashMapV8<K, LongAdder>();
    }

    /**
     * If the given key does not already exist in the table, inserts
     * the key with initial sum of zero; in either case returning the
     * adder associated with this key.
     * 
     * @param key the key
     * @return the adder associated with the key
     */
    public LongAdder install(K key) {
        return map.computeIfAbsent(key, createAdder);
    }

    /**
     * Adds the given value to the sum associated with the given
     * key. If the key does not already exist in the table, it is
     * inserted.
     * 
     * @param key the key
     * @param x the value to add
     */
    public void add(K key, long x) {
        map.computeIfAbsent(key, createAdder).add(x);
    }

    /**
     * Increments the sum associated with the given key. If the key
     * does not already exist in the table, it is inserted.
     * 
     * @param key the key
     */
    public void increment(K key) {
        add(key, 1L);
    }

    /**
     * Decrements the sum associated with the given key. If the key
     * does not already exist in the table, it is inserted.
     * 
     * @param key the key
     */
    public void decrement(K key) {
        add(key, -1L);
    }

    /**
     * Returns the sum associated with the given key, or zero if the
     * key does not currently exist in the table.
     * 
     * @param key the key
     * @return the sum associated with the key, or zero if the key is
     *         not in the table
     */
    public long sum(K key) {
        LongAdder a = map.get(key);
        return a == null ? 0L : a.sum();
    }

    /**
     * Resets the sum associated with the given key to zero if the key
     * exists in the table. This method does <em>NOT</em> add or
     * remove the key from the table (see {@link #remove}).
     * 
     * @param key the key
     */
    public void reset(K key) {
        LongAdder a = map.get(key);
        if(a != null)
            a.reset();
    }

    /**
     * Resets the sum associated with the given key to zero if the key
     * exists in the table. This method does <em>NOT</em> add or
     * remove the key from the table (see {@link #remove}).
     * 
     * @param key the key
     * @return the previous sum, or zero if the key is not
     *         in the table
     */
    public long sumThenReset(K key) {
        LongAdder a = map.get(key);
        return a == null ? 0L : a.sumThenReset();
    }

    /**
     * Returns the sum totalled across all keys.
     * 
     * @return the sum totalled across all keys
     */
    public long sumAll() {
        long sum = 0L;
        for (LongAdder a : map.values())
            sum += a.sum();
        return sum;
    }

    /**
     * Resets the sum associated with each key to zero.
     */
    public void resetAll() {
        for (LongAdder a : map.values())
            a.reset();
    }

    /**
     * Totals, then resets, the sums associated with all keys.
     * 
     * @return the sum totalled across all keys
     */
    public long sumThenResetAll() {
        long sum = 0L;
        for (LongAdder a : map.values())
            sum += a.sumThenReset();
        return sum;
    }

    /**
     * Removes the given key from the table.
     * 
     * @param key the key
     */
    public void remove(K key) {
        map.remove(key);
    }

    /**
     * Removes all keys from the table.
     */
    public void removeAll() {
        map.clear();
    }

    /**
     * Returns the current set of keys.
     * 
     * @return the current set of keys
     */
    public Set<K> keySet() {
        return map.keySet();
    }

    /**
     * Returns the current set of key-value mappings.
     * 
     * @return the current set of key-value mappings
     */
    public Set<Map.Entry<K, LongAdder>> entrySet() {
        return map.entrySet();
    }

}
