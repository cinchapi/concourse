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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.server.plugin.io.PluginSerializable;
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
public abstract class Dataset<E, A, V> extends AbstractMap<E, Map<A, Set<V>>>
        implements PluginSerializable, Insertable<E, A, V> {

    private static final long serialVersionUID = 7367380464340786513L;

    /**
     * A mapping from each attribute to the inverted (e.g. index-oriented) view
     * of the index.
     */
    private final Map<A, Map<V, Set<E>>> inverted;

    /**
     * A mapping from each entity to a primary (e.g. row-oriented) view of that
     * entity's data. Since this class is primarily used as a warehouse to
     * quickly produce {@link #invert(Object) inverted} views, each entity's
     * data is wrapped in a {@link SoftReference}, so care must be taken to
     * regenerate the row-oriented view on the fly, if necessary.
     */
    private final Map<E, SoftReference<Map<A, Set<V>>>> rows;

    /**
     * Construct a new instance.
     */
    public Dataset() {
        this.inverted = Maps.newHashMap();
        this.rows = Maps.newHashMap();
    }

    /**
     * Remove the association between {@code attribute} and {@code value} within
     * the {@code entity}.
     * 
     * @param entity the entity
     * @param attribute the attribute
     * @param value the value
     * @return {@code true} if the associated is removed
     */
    public boolean delete(E entity, A attribute, V value) {
        Map<V, Set<E>> index = inverted.get(attribute);
        if(index != null) {
            Set<E> entities = index.get(value);
            if(entities != null && entities.remove(entity)) {
                SoftReference<Map<A, Set<V>>> ref = rows.get(entity);
                Map<A, Set<V>> row = null;
                if(ref != null && (row = ref.get()) != null) {
                    row.get(attribute).remove(value);
                }
                return true;
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    @Override
    public Set<Entry<E, Map<A, Set<V>>>> entrySet() {
        Set<Entry<E, Map<A, Set<V>>>> entrySet = Sets.newLinkedHashSet();
        for (Entry<E, SoftReference<Map<A, Set<V>>>> entry : rows.entrySet()) {
            E entity = entry.getKey();
            Map<A, Set<V>> row = entry.getValue().get();
            if(row == null) {
                row = get(entity);
            }

            entrySet.add(new SimpleEntry<E, Map<A, Set<V>>>(entity, row));
        }
        return entrySet;
    }

    /**
     * Return all the values that are mapped from {@code attribute} within the
     * {@code entity}.
     * 
     * @param entity the entity
     * @param attribute the attribute
     * @return the set of values that are mapped
     */
    public Set<V> get(E entity, A attribute) {
        SoftReference<Map<A, Set<V>>> ref = rows.get(entity);
        Map<A, Set<V>> row = null;
        if(ref != null && (row = ref.get()) != null) {
            return row.get(attribute);
        }
        else {
            Set<V> values = Sets.newLinkedHashSet();
            Map<V, Set<E>> index = MoreObjects.firstNonNull(
                    inverted.get(attribute),
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

    @SuppressWarnings("unchecked")
    @Override
    public Map<A, Set<V>> get(Object entity) {
        SoftReference<Map<A, Set<V>>> sref = rows.get(entity);
        Map<A, Set<V>> row = null;
        if(sref != null && (row = sref.get()) == null) {
            row = Maps.newHashMap();
            for (Entry<A, Map<V, Set<E>>> entry : inverted.entrySet()) {
                A attr = entry.getKey();
                for (Entry<V, Set<E>> index : entry.getValue().entrySet()) {
                    Set<E> entities = index.getValue();
                    if(entities.contains(entity)) {
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
                rows.put((E) entity, new SoftReference<Map<A, Set<V>>>(row));
            }
        }
        return row;
    }

    /**
     * Add an association between {@code attribute} and {@code value} within the
     * {@code entity}.
     * 
     * @param entity the entity
     * @param attribute the attribute
     * @param value the value
     * @return {@code true} if the association can be added because it didn't
     *         previously exist
     */
    @Override
    public boolean insert(E entity, A attribute, V value) {
        Map<V, Set<E>> index = inverted.get(attribute);
        if(index == null) {
            index = createInvertedMultimap();
            inverted.put(attribute, index);
        }
        Set<E> entities = index.get(value);
        if(entities == null) {
            entities = Sets.newHashSet();
            index.put(value, entities);
        }
        entities = index.get(value); // NOTE: necessary to #get the inner set
                                     // again because TrackingMultimap uses
                                     // special internal collections
        if(entities.add(entity)) { //
            // Attempt to also add the data to the row-oriented view, if its
            // currently being held in memory
            SoftReference<Map<A, Set<V>>> ref = rows.get(entity);
            Map<A, Set<V>> row = null;
            if(ref != null && (row = ref.get()) != null) {
                Set<V> values = row.get(attribute);
                if(values == null) {
                    values = Sets.newLinkedHashSet();
                    row.put(attribute, values);
                    values = row.get(attribute);
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
     * Return an <em>inverted</em> view of the entire dataset.
     * <p>
     * An inverted view maps each attribute to a mapping from each contained
     * value to the set of entities in which that value is contained for the
     * attribute.
     * </p>
     * 
     * @return an inverted version of the entire dataset
     */
    public Map<A, Map<V, Set<E>>> invert() {
        return inverted;
    }

    /**
     * Return an <em>inverted</em> view of the data contained for
     * {@code attribute}.
     * <p>
     * For an attribute, an inverted view maps each contained value to the set
     * of entities in which that value is associated with the attribute.
     * </p>
     * 
     * @param attribute the attribute
     * @return an inverted version of the data for {@code attribute}
     */
    public Map<V, Set<E>> invert(A attribute) {
        return inverted.get(attribute);
    }

    @Override
    public Map<A, Set<V>> put(E entity, Map<A, Set<V>> mappings) {
        Map<A, Set<V>> current = get(entity);
        for (Entry<A, Set<V>> entry : mappings.entrySet()) {
            A attribute = entry.getKey();
            Set<V> values = entry.getValue();
            for (V value : values) {
                insert(entity, attribute, value);
            }
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<A, Set<V>> remove(Object entity) {
        Map<A, Set<V>> row = get(entity);
        for (Entry<A, Set<V>> entry : row.entrySet()) {
            A attribute = entry.getKey();
            Set<V> values = entry.getValue();
            for (V value : values) {
                delete((E) entity, attribute, value);
            }
        }
        return null;
    }

    /**
     * The subclass should return the proper {@link Map} from value to a
     * {@link Set} of entities.
     * 
     * @return the proper inverted multimap
     */
    protected abstract Map<V, Set<E>> createInvertedMultimap(); // TODO subclass
                                                                // should using
                                                                // TrackingMultimap

}
