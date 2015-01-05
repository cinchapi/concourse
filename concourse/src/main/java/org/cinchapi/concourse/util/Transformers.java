/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.annotate.UtilityClass;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A collection of tools used to transform objects in a collection using a
 * transformation {@link Function}.
 * 
 * @author jnelson
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
        Map<K2, Set<V2>> transformed = PrettyLinkedHashMap.newPrettyLinkedHashMap();
        for (Map.Entry<K, Set<V>> entry : original.entrySet()) {
            transformed.put(keys.apply(entry.getKey()),
                    transformSet(entry.getValue(), values));
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

}
