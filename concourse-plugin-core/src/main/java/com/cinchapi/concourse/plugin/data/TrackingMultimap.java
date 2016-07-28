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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zaxxer.sparsebits.SparseBitSet;

/**
 * An implementation of a {@code Map} that characterizes entries on the fly as
 * they are added or
 * removed. This is used to characterize user data as it is being entered, so
 * that the visualization
 * engine can query this map to immediately view data characterization in
 * constant time.
 * 
 * Apart from {@code #put(Object, Set)}, {@code #remove(Object)},
 * {@code #putAll(Map)}, and {@code #clear()},
 * all methods are delegated to an internal map. The four aforementioned methods
 * are overridden in terms of
 * functionality to characterize the entries in the map before performing the
 * original intended function.
 * 
 * {@link TrackingMultimap} is parametrized by type-parameters K and V, but the
 * underlying internal map is
 * in the form {@code Map<K, Set<V>>}. This is to comply with the format of
 * data, which is either a Map from
 * Strings (keys) to Objects (values), or Objects (values) to Longs (records).
 * 
 * @author Jeff Nelson
 */
// TODO talk about what is tracked for keys and what is tracked for values
@NotThreadSafe
public abstract class TrackingMultimap<K, V> extends AbstractMap<K, Set<V>> {

    /**
     * Return the correct {@link DataType} for the {@code clazz}.
     * 
     * @param clazz the {@link Class} to translate
     * @return the correct {@link DataType}
     */
    private static DataType getDataTypeForClass(Class<?> clazz) {
        if(Number.class.isAssignableFrom(clazz)
                || OTHER_NUMBER_CLASSES.contains(clazz)) {
            return DataType.NUMBER;
        }
        else if(clazz == String.class) {
            return DataType.STRING;
        }
        else if(clazz == Boolean.class || clazz == boolean.class) {
            return DataType.BOOLEAN;
        }
        else if(clazz == Link.class) {
            return DataType.LINK;
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
        this.keyTypes.put(DataType.NUMBER, new AtomicInteger(0));
        this.keyTypes.put(DataType.STRING, new AtomicInteger(0));
        this.keyTypes.put(DataType.BOOLEAN, new AtomicInteger(0));
        this.keyTypes.put(DataType.UNKNOWN, new AtomicInteger(0));
        this.totalValueCount = new AtomicLong(0);
        this.uniqueValueCount = new AtomicLong(0);
        this.valueCache = new SparseBitSet();
    }

    @Override
    public Set<Entry<K, Set<V>>> entrySet() {
        return data.entrySet();
    }

    public Map<DataType, Float> getPercentKeyDataTypes() {
        Map<DataType, Float> percents = Maps.newIdentityHashMap();
        /*
         * TODO do the work to get the percents
         */
        return percents;
    }

    /**
     * Returns whether the {@link TrackingMultimap} contains values of the specified
     * {@link DataType}.
     * 
     * @param type the {@link DataType} being queried
     * @return {@code true} if the {@code Map} contains this {@link DataType}, false otherwise
     */
    public boolean containsDataType(DataType type) {
        return getPercentKeyDataTypes().get(type) > 0;
    }

    /*
     * Object -> Set<Long>
     * record -> key -> set<values>
     * key -> value -> set<records>
     */

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
        int hashCode = value.hashCode();
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
            put(key, value);
        }
        return get(key);
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
            stored = new ValueSetWrapper();
            data.put(key, stored);
        }
        for (V element : stored) {
            remove(key, element);
        }
        for (V element : value) {
            put(key, element);
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
    public boolean put(K key, V value) {
        Set<V> values = data.get(key);
        if(values == null) {
            values = new ValueSetWrapper();
            data.put(key, values);
        }
        if(values.add(value)) {
            DataType keyType = getDataTypeForClass(key.getClass());
            keyTypes.get(keyType).incrementAndGet();
            // TODO: track more stats for keys, value tracking happens
            // in the ValueSetWrapper...
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Remove the association between {@code key} and {@code value} from the
     * map.
     * 
     * @param key the key
     * @param value the value
     * @return {@code true} if the association previously existed and is removed
     */
    public boolean remove(K key, V value) {
        Set<V> values = data.get(key);
        if(values != null && values.remove(value)) {
            DataType keyType = getDataTypeForClass(key.getClass());
            keyTypes.get(keyType).decrementAndGet();
            // TODO: track more stats for keys, value tracking happens
            // in the ValueSetWrapper
            if(values.isEmpty()) {
                data.remove(values);
            }
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
                remove((K) key, element); // type cast is valid because the
                                          // presence of elements over which to
                                          // iterate ensures that #put(K key, V
                                          // value) was called, which performs
                                          // type checking
            }
        }
        return stored;

    }

    @Override
    public Set<V> get(Object key) {
        return data.get(key);
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
        BOOLEAN, NUMBER, STRING, LINK, UNKNOWN;
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

        @Override
        public boolean add(V element) {
            boolean contained = hasValue(element);
            if(values.add(element)) {
                totalValueCount.incrementAndGet();
                if(!contained) {
                    // The value was not previously contained, so we must update
                    // the number of unique values stored across all the keys.
                    uniqueValueCount.incrementAndGet();
                    valueCache.set(element.hashCode());
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
