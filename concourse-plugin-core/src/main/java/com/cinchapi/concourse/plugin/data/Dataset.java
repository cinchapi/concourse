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

import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.io.Communicable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A data collection that associates the intersection of two keys, an
 * {@code entity} and {@code attribute} with a {@link Set} of values.
 * <p>
 * A {@link Dataset} may be sparse, where the number of total attributes is
 * vast, but the number that will apply to any given single entity is relatively
 * small. In this case, the Dataset is considered a sparse matrix, and this
 * class lends itself to space-efficient storage.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class Dataset<E, A, V> implements
        Map<E, Map<A, Set<V>>>,
        Communicable {

    /**
     * A mapping from each entity to a primary (e.g. row-oriented) view of that
     * entity's data. Since this class is primarily used as a warehouse to
     * quickly produce {@link #invert(Object) inverted} views, each entity's
     * data is wrapped in a {@link SoftReference}, so care must be taken to
     * regenerate the row-oriented view on the fly, if necessary.
     */
    private final Map<E, SoftReference<Map<A, Set<V>>>> rows;

    /**
     * A mapping from each attribute to the inverted (e.g. index-oriented) view
     * of the index.
     */
    private final Map<A, Map<V, Set<E>>> inverted;

    /**
     * TODO add docs
     * 
     * @param attr
     * @return
     */
    public Map<V, Set<E>> invert(A attr) {
        return Collections.unmodifiableMap(inverted.get(attr));
    }

    public enum DataType {
        NUMBER, BOOLEAN, STRING
    }

    // public class Column implements Map<C, Set<V>> {
    //
    // public float getPercentUniqueValues(){
    //
    // }
    //
    // public boolean isMultiValued(){
    //
    // }
    //
    // }

    /*
     * add method to internal map to get cardinality of each C globally
     * add method to internal map to get data type of each C globally and within
     * the row
     */

    private static final long serialVersionUID = 7367380464340786513L;

    public Dataset() {
        this.inverted = Maps.newHashMap();
        this.rows = Maps.newHashMap();
    }

    @Override
    public Set<Entry<E, Map<A, Set<V>>>> entrySet() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#size()
     */
    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    @Override
    public boolean containsKey(Object key) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    @Override
    public boolean containsValue(Object value) {
        // TODO Auto-generated method stub
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<A, Set<V>> get(Object key) {
        SoftReference<Map<A, Set<V>>> sref = rows.get(key);
        Map<A, Set<V>> row = null;
        if(sref != null && (row = sref.get()) == null) {
            row = Maps.newHashMap();
            for (Entry<A, Map<V, Set<E>>> entry : inverted.entrySet()) {
                A attr = entry.getKey();
                for (Entry<V, Set<E>> index : entry.getValue().entrySet()) {
                    Set<E> entities = index.getValue();
                    if(entities.contains(key)) {
                        V value = index.getKey();
                        Set<V> stored = row.get(attr);
                        if(stored == null) {
                            stored = Sets.newLinkedHashSet();
                            row.put(attr, stored);
                        }
                        stored.add(value);
                    }
                }
            }
            if(!row.isEmpty()) {
                // NOTE: Not worried about ClassCastException here because the
                // non-emptiness of the map guarantees that some data was added
                // for the key using the #put method, which performs type
                // checking
                rows.put((E) key, new SoftReference<Map<A, Set<V>>>(row));
            }
        }
        return row;
    }

    /**
     * TODO add docs
     * 
     * @param entity
     * @param attr
     * @param value
     * @return
     */
    public boolean put(E entity, A attr, V value) {
        Map<V, Set<E>> index = inverted.get(attr);
        if(index == null) {
            index = createInvertedMultimap();
            inverted.put(attr, index);
        }
        Set<E> entities = index.get(value);
        if(entities == null) {
            entities = Sets.newHashSet();
            index.put(value, entities);
        }
        if(entities.add(entity)) {
            // Attempt to also add the data to the row-oriented view, if its
            // currently being held in memory
            SoftReference<Map<A, Set<V>>> ref = rows.get(entity);
            Map<A, Set<V>> row = null;
            if(ref != null && (row = ref.get()) != null) {
                Set<V> values = row.get(attr);
                if(values == null) {
                    values = Sets.newLinkedHashSet();
                    row.put(attr, values);
                    values = row.get(attr);
                }
                values.add(value);
            }
            return true;
        }
        else {
            return false;
        }

    }

    /**
     * TODO add docs
     * 
     * @param entity
     * @param attr
     * @return
     */
    public Set<V> get(E entity, A attr) {
        SoftReference<Map<A, Set<V>>> ref = rows.get(entity);
        Map<A, Set<V>> row = null;
        if(ref != null && (row = ref.get()) != null) {
            return row.get(attr);
        }
        else {
            Set<V> values = Sets.newLinkedHashSet();
            Map<V, Set<E>> index = MoreObjects.firstNonNull(inverted.get(attr),
                    Collections.<V, Set<E>> emptyMap());
            for (Entry<V, Set<E>> entry : index.entrySet()) {
                Set<E> entities = entry.getValue();
                if(entities.contains(entity)) {
                    V value = entry.getKey();
                    values.add(value);
                }
            }
            return values;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    @Override
    public Map<A, Set<V>> put(E key, Map<A, Set<V>> value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#remove(java.lang.Object)
     */
    @Override
    public Map<A, Set<V>> remove(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#putAll(java.util.Map)
     */
    @Override
    public void putAll(Map<? extends E, ? extends Map<A, Set<V>>> m) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#clear()
     */
    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#keySet()
     */
    @Override
    public Set<E> keySet() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#values()
     */
    @Override
    public Collection<Map<A, Set<V>>> values() {
        // TODO Auto-generated method stub
        return null;
    }
    
    protected abstract Map<V, Set<E>> createInvertedMultimap(); //TODO subclass should using TrackingMultimap

}
