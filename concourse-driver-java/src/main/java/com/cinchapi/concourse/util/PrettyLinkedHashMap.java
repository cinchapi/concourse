/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;

/**
 * A special {@link LinkedHashMap} with a named key and value that returns a
 * pretty {@link #toString()} representation in the form of a two column table
 * where the key/value pairs are listed in different rows :
 * 
 * <pre>
 * +-------------------------------+
 * | Key  | Values                 |
 * +-------------------------------+
 * | age  | [1]                    |
 * | foo  | [1]                    |
 * | name | [Jeff Nelson, jeffery] |
 * +-------------------------------+
 * </pre>
 * <p>
 * A {@link PrettyLinkedHashMap} is suitable to use when displaying information
 * about a single record or document.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class PrettyLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    /**
     * Return an empty {@link Map} that "prettifies" its {@link #toString()
     * string} representation on-demand.
     * 
     * @return the new {@link Map}
     */
    public static <K, V> Map<K, V> create() {
        return of(ImmutableMap.of(), null, null);
    }

    /**
     * Return an empty {@link Map} that "prettifies" its {@link #toString()
     * string} representation on-demand.
     * <p>
     * {@code keyColumnHeader} is used as the header for the column that
     * contains the the keys and {@code valueColumnHeader} is used for the
     * column that contains the values.
     * </p>
     * 
     * @param keyColumnHeader
     * @param valueColumnHeader
     * @return the new {@link Map}
     */
    public static <K, V> Map<K, V> create(String keyColumnHeader,
            String valueColumnHeader) {
        return of(ImmutableMap.of(), keyColumnHeader, valueColumnHeader);
    }

    /**
     * Return a new {@link Map} containing all the entries from {@code data}
     * that "prettifies" its {@link #toString() string} representation
     * on-demand.
     * <p>
     * The returned {@link Map} copies, but does not "read through" to
     * {@code data}. It is mutable, but changes made to it are not reflected in
     * {@code data} and vice versa.
     * </p>
     * 
     * @param data
     * @return a {@link Map} containing {@code data} that has pretty
     *         {@link #toString()} output
     */
    public static <K, V> Map<K, V> of(Map<K, V> data) {
        return of(data, null, null);
    }

    /**
     * Return a new {@link Map} containing all the entries from {@code data}
     * that "prettifies" its {@link #toString() string} representation
     * on-demand.
     * <p>
     * The returned {@link Map} copies, but does not "read through" to
     * {@code data}. It is mutable, but changes made to it are not reflected in
     * {@code data} and vice versa.
     * </p>
     * <p>
     * {@code keyColumnHeader} is used as the header for the column that
     * contains the the keys and {@code valueColumnHeader} is used for the
     * column that contains the values.
     * </p>
     * 
     * @param data
     * @param keyColumnHeader
     * @param valueColumnHeader
     * @return a {@link Map} containing {@code data} that has pretty
     *         {@link #toString()} output
     */
    public static <K, V> Map<K, V> of(Map<K, V> data, String keyColumnHeader,
            String valueColumnHeader) {
        return new LazyPrettyLinkedHashMap<>(data, keyColumnHeader,
                valueColumnHeader);
    }

    /**
     * Return an empty {@link PrettyLinkedHashMap} with the default key and
     * value names.
     * 
     * @return the PrettyLinkedHashMap
     * @deprecated use {@link #create()} instead
     */
    @Deprecated
    public static <K, V> PrettyLinkedHashMap<K, V> newPrettyLinkedHashMap() {
        return new PrettyLinkedHashMap<K, V>(null, null);
    }

    /**
     * Return an empty TLinkedHashMap with the specified {@code keyColumnHeader}
     * and {@code valueColumnHeader}.
     * 
     * @param keyColumnHeader
     * @param valueColumnHeader
     * @return the PrettyLinkedHashMap.
     * @deprecated use {@link #create(String, String)} instead
     */
    @Deprecated
    public static <K, V> PrettyLinkedHashMap<K, V> newPrettyLinkedHashMap(
            String keyColumnHeader, String valueColumnHeader) {
        return new PrettyLinkedHashMap<K, V>(keyColumnHeader,
                valueColumnHeader);
    }

    private static final long serialVersionUID = 1L; // serializability
                                                     // inherited from parent
                                                     // class

    private String keyColumnHeader = "Key";
    private String valueColumnHeader = "Value";
    private int keyColumnLength = keyColumnHeader.length();
    private int valueColumnLength = valueColumnHeader.length();

    /**
     * Construct a new instance.
     * 
     * @param keyColumnHeader
     * @param valueColumnHeader
     */
    private PrettyLinkedHashMap(@Nullable String keyColumnHeader,
            @Nullable String valueColumnHeader) {
        if(!Strings.isNullOrEmpty(keyColumnHeader)) {
            setKeyName(keyColumnHeader);
        }
        if(!Strings.isNullOrEmpty(valueColumnHeader)) {
            setValueName(valueColumnHeader);
        }
    }

    @Override
    public V put(K key, V value) {
        keyColumnLength = Math.max(key.toString().length(), keyColumnLength);
        valueColumnLength = Math.max(
                value != null ? value.toString().length() : 4,
                valueColumnLength);
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        map.forEach((key, value) -> put(key, value));
    }

    /**
     * Set the keyColumnHeader to {@code name}.
     * 
     * @param name
     * @return this
     */
    public PrettyLinkedHashMap<K, V> setKeyName(String name) {
        keyColumnHeader = name;
        keyColumnLength = Math.max(name.length(), keyColumnLength);
        return this;
    }

    /**
     * Set the valueColumnHeader to {@code name}
     * 
     * @param name
     * @return this
     */
    public PrettyLinkedHashMap<K, V> setValueName(String name) {
        valueColumnHeader = name;
        valueColumnLength = Math.max(name.length(), valueColumnLength);
        return this;
    }

    @Override
    public String toString() {
        String format = "| %-" + keyColumnLength + "s | %-" + valueColumnLength
                + "s |%n";
        String hr = Strings.padEnd("+", keyColumnLength + valueColumnLength + 6,
                '-'); // there are 6 spaces in the #format
        hr += "+" + System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        sb.append(System.getProperty("line.separator"));
        sb.append(hr);
        sb.append(String.format(format, keyColumnHeader, valueColumnHeader));
        sb.append(hr);
        for (Map.Entry<K, V> entry : entrySet()) {
            sb.append(String.format(format, entry.getKey(), entry.getValue()));
        }
        sb.append(hr);
        return sb.toString();
    }

    /**
     * A wrapper around a map that generates a
     * {@link PrettyLinkedHashMap} for its data on the fly, when necessary
     * (e.g. {@link #toString()} and any attempts to mutate the original
     * source).
     *
     * @author Jeff Nelson
     */
    private static class LazyPrettyLinkedHashMap<K, V>
            extends ForwardingMap<K, V> {

        /**
         * The existing source that it passed in to be {@link #toString()
         * prettified} later.
         */
        private Map<K, V> source;

        /**
         * The header to use for the column that contains the keys.
         */
        private final String keyColumnHeader;

        /**
         * The header to use for the column that contains the values.
         */
        private final String valueColumnHeader;

        private LazyPrettyLinkedHashMap(Map<K, V> source,
                String keyColumnHeader, String valueColumnHeader) {
            this.source = source;
            this.keyColumnHeader = keyColumnHeader;
            this.valueColumnHeader = valueColumnHeader;
        }

        @Override
        public void clear() {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            pretty().clear();
        }

        @Override
        public V put(K key, V value) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            return pretty().put(key, value);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> map) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            pretty().putAll(map);
        }

        @Override
        public V remove(Object object) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            return pretty().remove(object);
        }

        @Override
        public String toString() {
            return pretty().toString();
        }

        @Override
        protected Map<K, V> delegate() {
            return source;
        }

        /**
         * Return a {@link Map} with all the contained data that is guaranteed
         * to be pretty.
         * 
         * @return a pretty map
         */
        private Map<K, V> pretty() {
            if(!(source instanceof PrettyLinkedHashMap)) {
                Map<K, V> staging = new PrettyLinkedHashMap<>(keyColumnHeader,
                        valueColumnHeader);
                staging.putAll(source);
                source = staging;
            }
            return source;
        }

    }
}
