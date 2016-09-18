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
package com.cinchapi.concourse.plugin.data;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.plugin.io.PluginSerializable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zaxxer.sparsebits.SparseBitSet;

/**
 * <p>
 * An implementation of a {@code Map} that characterizes entries on the fly as
 * they are added or removed. This is used to characterize user data as it is
 * being entered, so that the visualization engine can query this map to
 * immediately view data characterization in constant time.
 * </p>
 * <p>
 * Apart from {@link #put(Object, Set)}, {@link #remove(Object)},
 * {@link #putAll(Map)}, and {@link #clear()}, all methods are delegated to an
 * internal map. The four aforementioned methods are overridden in terms of
 * functionality to characterize the entries in the map before performing the
 * original intended function.
 * </p>
 * <p>
 * {@link TrackingMultimap} is parametrized by type-parameters K and V, but the
 * underlying internal map is in the form {@code Map<K, Set<V>>}. This is to
 * comply with the format of data, which is either a Map from Strings (keys) to
 * Objects (values), or Objects (values) to Longs (records).
 * </p>
 * 
 * @author Jeff Nelson
 */
// TODO talk about what is tracked for keys and what is tracked for values
@NotThreadSafe
public abstract class TrackingMultimap<K, V> extends AbstractMap<K, Set<V>> implements
        PluginSerializable {

    private static final long serialVersionUID = -1387614861194624711L;

    /**
     * Return the correct {@link DataType} for the {@code clazz}.
     * 
     * @param clazz the {@link Class} to translate
     * @return the correct {@link DataType}
     */
    private static DataType getDataTypeForClass(Class<?> clazz) {
        if(clazz == Link.class) {
            return DataType.LINK;
        }
        else if((Number.class.isAssignableFrom(clazz) || OTHER_NUMBER_CLASSES
                .contains(clazz))) {
            return DataType.NUMBER;
        }
        else if(clazz == String.class) {
            return DataType.STRING;
        }
        else if(clazz == Boolean.class || clazz == boolean.class) {
            return DataType.BOOLEAN;
        }
        else {
            return DataType.UNKNOWN;
        }
    }

    /**
     * A collection of classes that don't extend {@link Number} should be
     * considered {@link DataType#NUMBER numerical}.
     */
    private static Set<Class<?>> OTHER_NUMBER_CLASSES = Sets
            .newIdentityHashSet();
    static {
        OTHER_NUMBER_CLASSES.add(int.class);
        OTHER_NUMBER_CLASSES.add(long.class);
        OTHER_NUMBER_CLASSES.add(float.class);
        OTHER_NUMBER_CLASSES.add(double.class);
        OTHER_NUMBER_CLASSES.add(short.class);
        OTHER_NUMBER_CLASSES.add(byte.class);
    }

    /**
     * An internal map where the data is actually stored.
     */
    private Map<K, Set<V>> data;

    /**
     * A mapping from each of the {@link DataType data types} to the number of
     * stored keys that are characterized as such.
     */
    private final Map<DataType, AtomicInteger> keyTypes;

    /**
     * The total number of values (including duplicates) added across all the
     * keys.
     */
    private final AtomicLong totalValueCount;

    /**
     * The total number of unique values (e.g. excluding duplicates) that are
     * stored across all the keys.
     */
    private final AtomicLong uniqueValueCount;

    /**
     * An approximate cache of values stored across all the keys.
     * <p>
     * Whenever a value is added to the map, the bit for its
     * {@link Object#hashCode() hash code} is flipped to indicate that the value
     * is stored. However, hash codes are not guaranteed to be unique among
     * objects, so its necessary to look through all the values and test the
     * equality for a potential match to determine if an object is actually
     * contained or not.
     * </p>
     */
    private final SparseBitSet valueCache;

    /**
     * Construct a new instance.
     * 
     * @param delegate an {@link Map#isEmpty() empty} map
     */
    protected TrackingMultimap(Map<K, Set<V>> delegate) {
        Preconditions.checkState(delegate.isEmpty());
        this.data = delegate;
        this.keyTypes = Maps.newIdentityHashMap();
        for (DataType type : DataType.values()) {
            this.keyTypes.put(type, new AtomicInteger(0));
        }
        this.totalValueCount = new AtomicLong(0);
        this.uniqueValueCount = new AtomicLong(0);
        this.valueCache = new SparseBitSet();
    }

    /**
     * Returns whether the {@link TrackingMultimap} contains values of the
     * specified {@link DataType}.
     * 
     * @param type the {@link DataType} being queried
     * @return {@code true} if the {@code Map} contains this {@link DataType},
     *         false otherwise
     */
    public boolean containsDataType(DataType type) {
        return percentKeyDataType(type) > 0;
    }

    /**
     * Remove the association between {@code key} and {@code value} from the
     * map.
     * 
     * @param key the key
     * @param value the value
     * @return {@code true} if the association previously existed and is removed
     */
    public boolean delete(K key, V value) {
        Set<V> values = data.get(key);
        if(values != null && values.remove(value)) {
            if(values.isEmpty()) {
                data.remove(values);
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Set<Entry<K, Set<V>>> entrySet() {
        return data.entrySet();
    }

    @Override
    public Set<V> get(Object key) {
        return data.get(key);
    }

    /**
     * Return {@code true} if this map associates {@code value} with at least
     * one key.
     * <p>
     * This method is different from {@link #containsValue(Object)} because it
     * checks for values <strong>within</strong> the Sets that are mapped from
     * keys. Use the aforementioned if you need to check for the existence of an
     * entire Set as opposed to an individual value.
     * </p>
     * 
     * @param value the value to checks
     * @return {@code true} if the value is contained, {@code false} otherwise
     */
    public boolean hasValue(V value) {
        int hashCode = Math.abs(value.hashCode());
        if(valueCache.get(hashCode)) {
            for (Set<V> values : data.values()) {
                if(values.contains(value)) {
                    return true;
                }
            }
            return false;
        }
        else {
            return false;
        }
    }

    /**
     * Merge all the {@code values} into the set of values that is mapped from
     * {@code key}.
     * 
     * @param key the key
     * @param values the values to merge
     * @return all the values mapped from {@code key} after the merge
     */
    public Set<V> merge(K key, Set<V> values) {
        for (V value : values) {
            insert(key, value);
        }
        return get(key);
    }

    /**
     * Return the percent (between 0 and 1) of keys that are an instance of the
     * specified {@link DataType type}.
     * 
     * @param type the {@link DataType} of interest
     * @return the percent of keys of the {@code type}
     */
    public double percentKeyDataType(DataType type) {
        return ((double) keyTypes.get(type).get()) / totalValueCount.get();
    }

    /**
     * Determines the proportion of occurrence of a particular key. This is
     * merely the frequency of that key divided by the total number of key
     * frequencies.
     * 
     * @param element the key for which the proportion is being sought
     * @return the proportion of the key
     */
    public double proportion(K element) {
        double frequency = data.get(element).size();
        return frequency / totalValueCount.get();
    }

    /**
     * <p>
     * <strong>NOTE:</strong> This implementation will replace all the existing
     * values mapped from {@code key} with those specified in the {@code value}.
     * If you want "merge-like" functionality call the {@link #merge(Set)}
     * method.
     * </p>
     * {@inheritDoc}
     */
    @Override
    public Set<V> put(K key, Set<V> value) {
        Set<V> stored = data.get(key);
        if(stored == null) {
            stored = new ValueSetWrapper(key);
            data.put(key, stored);
        }
        for (V element : stored) {
            delete(key, element);
        }
        for (V element : value) {
            insert(key, element);
        }
        return stored;
    }

    /**
     * Add a new association between {@code key} and {@code value} to the map if
     * it doesn't already exist.
     * 
     * @param key the key
     * @param value the value
     * @return {@code true} if the association didn't previously exist and is
     *         not added
     */
    public boolean insert(K key, V value) {
        Set<V> values = data.get(key);
        if(values == null) {
            values = new ValueSetWrapper(key);
            data.put(key, values);
        }
        if(values.add(value)) {
            return true;
        }
        else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<V> remove(Object key) {
        Set<V> stored = data.get(key);
        if(stored != null) {
            for (V element : stored) {
                delete((K) key, element); // type cast is valid because the
                                          // presence of elements over which to
                                          // iterate ensures that #put(K key, V
                                          // value) was called, which performs
                                          // type checking
            }
        }
        return stored;

    }

    /**
     * Calculates the uniqueness of the data by summing the squares of the
     * proportions of each key within the {@link #keySet() key set},
     * determining the square root of the sum, and subtracting it from 1. This
     * always results in a number between 0 and 1.
     * <p>
     * For datasets with a large number of distinct values appearing in
     * relatively similar frequency, this function returns a relatively high
     * number, since there are many unique values. Mathematically, each
     * contributes a small amount to the proportion, so the square root term is
     * small, returning a large end result.
     * </p>
     * <p>
     * Conversely, for datasets with a few dominating values, this function
     * returns a fairly low number. This is because the higher proportions from
     * the dominating values contribute more heavily towards the sum of squares.
     * The square root is therefore higher, and when subtracted from 1, returns
     * a lower number.
     * </p>
     * 
     * @return the uniqueness of the data, on a scale from 0 to 1.
     */
    public double uniqueness() {
        double sumOfSquares = 0;
        for (K key : this.keySet()) {
            sumOfSquares += Math.pow(proportion(key), 2);
        }
        return 1 - Math.sqrt(sumOfSquares);
    }

    /**
     * Determines how many unique values exist within the {@link Map} and
     * returns the appropriate {@link VariableType}.
     * 
     * The three possible return types are:
     * <ol>
     * <li><strong>DICHOTOMOUS</strong>: if there are 1 or 2 unique values</li>
     * <li><strong>NOMINAL</strong>: if the number of unique values is greater
     * than 2 and less than or equal to 12</li>
     * <li><strong>INTERVAL</strong>: if there are more than 12 unique values</li>
     * </ol>
     * 
     * @return
     */
    public VariableType variableType() {
        // NOTE: The boundary between nominal and interval is arbitrary, and may
        // require tweaking since it is a heuristic model.
        if(data.keySet().size() <= 2) {
            return VariableType.DICHOTOMOUS;
        }
        else if(data.keySet().size() <= 12) {
            return VariableType.NOMINAL;
        }
        else {
            return VariableType.INTERVAL;
        }
    }

    /**
     * Return a new {@link Set} (of the appropriate type) to use for storing the
     * values that are mapped from a key.
     * 
     * @return a new {@link Set}
     */
    protected abstract Set<V> createValueSet();

    /**
     * A broad classification of objects that describes the nature of the data.
     * 
     * @author Jeff Nelson
     */
    public static enum DataType {
        BOOLEAN, LINK, NUMBER, STRING, UNKNOWN;
    }

    /**
     * A classification of objects that describes how data is categorized
     */
    public static enum VariableType {
        DICHOTOMOUS, INTERVAL, NOMINAL;
    }

    /**
     * An internal wrapper around a Set returned from the
     * {@link #createValueSet()} method.
     * <p>
     * The wrapper is responsible for tracking stats for the individual set and
     * updating the appropriate variables of the outer class. This ensures that
     * the caller can interact with individual value sets without breaking
     * tracking semantics.
     * </p>
     * 
     * @author Jeff Nelson
     */
    private class ValueSetWrapper extends AbstractSet<V> {

        /**
         * The wrapped set that actually stores the data.
         */
        private final Set<V> values = createValueSet();

        /**
         * The key from which this {@link Set} is mapped in the outer
         * TrackingMultimap.
         */
        private K key;

        /**
         * Construct a new instance.
         * 
         * @param key
         */
        ValueSetWrapper(K key) {
            this.key = key;
        }

        @Override
        public boolean add(V element) {
            boolean contained = hasValue(element);
            if(values.add(element)) {
                totalValueCount.incrementAndGet();
                DataType keyType = getDataTypeForClass(key.getClass());
                keyTypes.get(keyType).incrementAndGet();
                if(!contained) {
                    // The value was not previously contained, so we must update
                    // the number of unique values stored across all the keys.
                    uniqueValueCount.incrementAndGet();
                    valueCache.set(Math.abs(element.hashCode()));
                }
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public Iterator<V> iterator() {

            return new Iterator<V>() {

                /**
                 * The delegate iterator that controls state.
                 */
                private final Iterator<V> delegate = values.iterator();

                /**
                 * The last value returned from the {@link #next()} method.
                 */
                private V next = null;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public V next() {
                    next = delegate.next();
                    return next;
                }

                @Override
                public void remove() {
                    ValueSetWrapper.this.remove(next);
                    next = null;
                }

            };
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean remove(Object element) {
            if(values.remove(element)) {
                totalValueCount.decrementAndGet();
                DataType keyType = getDataTypeForClass(key.getClass());
                keyTypes.get(keyType).decrementAndGet();
                boolean contained = hasValue((V) element);
                if(!contained) {
                    // Since the value is no longer "contained" we are free to
                    // decrement the number of unique values stored across all
                    // the keys
                    uniqueValueCount.decrementAndGet();
                }
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public int size() {
            return values.size();
        }
    }

}
