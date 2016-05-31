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
package com.cinchapi.concourse.util;

import java.lang.reflect.Array;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.annotate.UtilityClass;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A collection of tools used to transform objects in a collection using a
 * transformation {@link Function}.
 * 
 * @author Jeff Nelson
 */
@UtilityClass
public final class Transformers {

    /**
     * Return an array whose content is equal to the content of {@code original}
     * after it has been transformed by {@code function}.
     * 
     * @param original
     * @param function
     * @param vClass - the class object for the type to which the items in
     *            {@code original} will be transformed
     * @return the transformed array
     */
    @SuppressWarnings("unchecked")
    public static <F, V> V[] transformArray(F[] original,
            Function<? super F, ? extends V> function, Class<V> vClass) {
        V[] transformed = (V[]) Array.newInstance(vClass, original.length);
        for (int i = 0; i < original.length; ++i) {
            F item = original[i];
            transformed[i] = function.apply(item);
        }
        return transformed;
    }

    /**
     * Return a Map whose keys are equal to the those in {@code original} after
     * it has been transformed by {@code function}.
     * <p>
     * <strong>WARNING:</strong> There is the potential for data loss in the
     * event that {@code function} returns duplicate transformed results for
     * items in {@code original}.
     * </p>
     * 
     * @param original
     * @param function
     * @return the transformed Map
     */
    public static <K, K2, V> Map<K2, V> transformMap(Map<K, V> original,
            Function<? super K, ? extends K2> function) {
        Map<K2, V> transformed = PrettyLinkedHashMap.newPrettyLinkedHashMap();
        for (Map.Entry<K, V> entry : original.entrySet()) {
            transformed.put(function.apply(entry.getKey()), entry.getValue());
        }
        return transformed;
    }

    /**
     * Transform the keys in {@code original} with the {@code keys} function
     * and each of the values with the {@code values} function and return the
     * result.
     * <p>
     * <strong>WARNING:</strong> There is the potential for data loss in the
     * event that {@code function} returns duplicate transformed results for
     * items in {@code original}.
     * </p>
     * 
     * @param original
     * @param keys
     * @param values
     * @return the transformed Map
     */
    public static <K, K2, V, V2> Map<K2, Set<V2>> transformMapSet(
            Map<K, Set<V>> original, Function<? super K, ? extends K2> keys,
            Function<? super V, ? extends V2> values) {
        Map<K2, Set<V2>> transformed = Maps.newLinkedHashMap();
        for (Map.Entry<K, Set<V>> entry : original.entrySet()) {
            transformed.put(keys.apply(entry.getKey()),
                    transformSet(entry.getValue(), values));
        }
        return transformed;
    }

    /**
     * Transform each of the values in the {@code original} with the
     * {@code function}.
     * 
     * @param original
     * @param function
     * @return the transformed Map
     */
    public static <K, V, V2> Map<K, V2> transformMapValues(Map<K, V> original,
            Function<V, V2> function) {
        Map<K, V2> transformed = Maps.newLinkedHashMap();
        for (Map.Entry<K, V> entry : original.entrySet()) {
            transformed.put(entry.getKey(), function.apply(entry.getValue()));
        }
        return transformed;
    }

    /**
     * Populate {@code transformed} with the items in {@code original} after
     * applying {@code function}.
     * <p>
     * <strong>WARNING:</strong> There is the potential for data loss in the
     * event that {@code function} returns duplicate transformed results for
     * items in {@code original}.
     * </p>
     * 
     * @param original
     * @param function
     * @return the transformed Set
     */
    public static <F, V> Set<V> transformSet(Set<F> original,
            Function<? super F, ? extends V> function) {
        Set<V> transformed = Sets.newLinkedHashSetWithExpectedSize(original
                .size());
        for (F item : original) {
            transformed.add(function.apply(item));
        }
        return transformed;
    }
    /**
     * Populate {@code transformed} with the items in {@code original} after
     * applying Lazy transform
     * @param original
     * @param function
     * @return the transformed Set
     */
    public static <V1, V2> Set<V2> transformSetLazily(Set<V1> original, Function<V1, V2> function) {

        return new LazyTransformSet<V1, V2>(original, function);
    }

    /**
     * Transform the keys in {@code original} with the {@code keys} function
     * and each of the values with the {@code values} function and return the
     * map result that is sorted according to the {@code sorter}.
     * <p>
     * <strong>WARNING:</strong> There is the potential for data loss in the
     * event that {@code function} returns duplicate transformed results for
     * items in {@code original}.
     * </p>
     * 
     * @param original
     * @param keys
     * @param values
     * @param sorter
     * @return the transformed TreeMap
     */
    public static <K, K2, V, V2> Map<K2, Set<V2>> transformTreeMapSet(
            Map<K, Set<V>> original, Function<? super K, ? extends K2> keys,
            Function<? super V, ? extends V2> values,
            final Comparator<K2> sorter) {
        Map<K2, Set<V2>> transformed = Maps.newTreeMap(sorter);
        for (Map.Entry<K, Set<V>> entry : original.entrySet()) {
            transformed.put(keys.apply(entry.getKey()),
                    transformSet(entry.getValue(), values));
        }
        return transformed;
    }
    
    /**
     * A {@link Set} that transform values from an original set of type {@code V1} into values of type {@code V2}
     * on-the-fly using a transformation {@link Function}.
     * 
     * @author chandresh.pancholi
     *
     */
    public static class LazyTransformSet<V1, V2> extends AbstractSet<V2>  {
        
        /**
         * Original set V1
         */
        private final Set<V1> original;
        
        /**
         * Guava transform function. function's apply method transform V1 to V2 by apply function
         */
        private final Function<V1, V2> function;

        public LazyTransformSet(Set<V1> original, Function<V1, V2> function){
            this.original = original;
            this.function = function;
        }

        
        @Override
        public Iterator<V2> iterator() {
            return new Iterator<V2>(){
                Iterator<V1> backing = original.iterator();
               
                @Override
                public boolean hasNext() {
                    return backing.hasNext();
                }
                
                @Override
                public V2 next() {
                     V1 backingNext = backing.next();

                     return function.apply(backingNext);
                }
            };
        }

        /**
         * @return size of original set
         */
        @Override
        public int size() {
            return original.size();
        }
    }

}
