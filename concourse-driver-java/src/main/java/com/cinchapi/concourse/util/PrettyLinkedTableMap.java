/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
import com.google.common.collect.Maps;

/**
 * A special {@link LinkedHashMap} that simulates a sparse table where a row key
 * maps to a set of columns and each column maps to a value (e.g. Map[R, ->
 * Map[C -> V]]). This object has pretty {@link #toString()} output that is
 * formatted as such:
 * 
 * <pre>
 * +-----------------------------------------------------------+
 * | Record           | name          | gender      | joinedBy |
 * +-----------------------------------------------------------+
 * | 1416242356271000 | Deary Hudson  | UNSPECIFIED | EMAIL    |
 * | 1416242356407000 | Jeff Nelson   | UNSPECIFIED | EMAIL    |
 * | 1416242356436000 | Morgan Debaun | UNSPECIFIED | EMAIL    |
 * +-----------------------------------------------------------+
 * </pre>
 * <p>
 * A {@link PrettyLinkedTableMap} is suitable for displaying information about
 * multiple records or documents.
 * </p>
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class PrettyLinkedTableMap<R, C, V> extends LinkedHashMap<R, Map<C, V>>
        implements
        PrettyTableMap<R, C, V> {

    /**
     * Return an empty {@link Map} with "pretty" {@link #toString()
     * string} representation, using the default identifier name.
     * 
     * @return the new {@link Map}
     */
    public static <R, C, V> PrettyTableMap<R, C, V> create() {
        return new PrettyLinkedTableMap<>(null);
    }

    /**
     * Return an empty {@link Map} with "pretty" {@link #toString()
     * string} representation.
     * <p>
     * {@code identifierColumnHeader} is used as the header for the column that
     * contains the the identifier for each row in the "table".
     * </p>
     * 
     * @param identifierColumnHeader
     * @return the new {@link Map}
     */
    public static <R, C, V> PrettyTableMap<R, C, V> create(
            String identifierColumnHeader) {
        return new PrettyLinkedTableMap<>(identifierColumnHeader);
    }

    /**
     * Return an empty {@link PrettyLinkedTableMap} with the default row name.
     * 
     * @return the PrettyLinkedTableMap
     * @deprecated use {@link #create()} instead
     */
    @Deprecated
    public static <R, C, V> PrettyLinkedTableMap<R, C, V> newPrettyLinkedTableMap() {
        return new PrettyLinkedTableMap<R, C, V>(null);
    }

    /**
     * Return an empty {@link PrettyLinkedTableMap} with the specified
     * {@code identifierColumnHeader}.
     * 
     * @param identifierColumnHeader
     * @return the PrettyLinkedTableMap
     * @deprecated use {@link #create(String)} instead
     */
    @Deprecated
    public static <R, C, V> PrettyLinkedTableMap<R, C, V> newPrettyLinkedTableMap(
            String identifierColumnHeader) {
        return new PrettyLinkedTableMap<R, C, V>(identifierColumnHeader);
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
    public static <R, C, V> PrettyTableMap<R, C, V> of(Map<R, Map<C, V>> data) {
        return of(data, null);
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
     * {@code identifierColumnHeader} is used as the header for the column that
     * contains the the identifier for each row in the "table".
     * </p>
     * 
     * @param data
     * @param identifierColumnHeader
     * @return a {@link Map} containing {@code data} that has pretty
     *         {@link #toString()} output
     */
    public static <R, C, V> PrettyTableMap<R, C, V> of(Map<R, Map<C, V>> data,
            String identifierColumnHeader) {
        return new LazyPrettyLinkedTableMap<>(data, identifierColumnHeader);

    }

    /**
     * The minimum column length. Is set equal to the size of the string "null"
     * for consistent spacing when values don't exist.
     */
    private static int MIN_COLUMN_LENGTH = "null".length();

    /**
     * A mapping from column to display length, which is equal to the greater of
     * the length of the largest value that will be displayed in the column and
     * the value of the column's toString() value.
     */
    private final Map<C, Integer> columns = Maps.newLinkedHashMap();

    private String identifierColumnHeader = "Row";
    private int identifierColumnLength = identifierColumnHeader.length();

    /**
     * Construct a new instance.
     * 
     * @param identifierColumnHeader
     */
    private PrettyLinkedTableMap(@Nullable String identifierColumnHeader) {
        if(!Strings.isNullOrEmpty(identifierColumnHeader)) {
            setIdentifierColumnHeader(identifierColumnHeader);
        }
    }

    @Override
    public V put(R row, C column, V value) {
        Map<C, V> rowdata = super.get(row);
        if(rowdata == null) {
            rowdata = Maps.newLinkedHashMap();
            super.put(row, rowdata);
        }
        identifierColumnLength = Math.max(row.toString().length(),
                identifierColumnLength);
        int current = columns.containsKey(column) ? columns.get(column)
                : MIN_COLUMN_LENGTH;
        columns.put(column, Math.max(current, Math
                .max(column.toString().length(), value.toString().length())));
        return rowdata.put(column, value);
    }

    /**
     * <p>
     * THIS METHOD ALWAYS RETURNS {@code null}.
     * </p>
     * {@inheritDoc}
     */
    @Override
    public Map<C, V> put(R key, Map<C, V> value) {
        for (Map.Entry<C, V> entry : value.entrySet()) {
            put(key, entry.getKey(), entry.getValue());
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends R, ? extends Map<C, V>> m) {
        m.forEach((key, value) -> put(key, value));
    }

    @Override
    public PrettyLinkedTableMap<R, C, V> setIdentifierColumnHeader(
            String header) {
        identifierColumnHeader = header;
        identifierColumnLength = Math.max(header.length(),
                identifierColumnLength);
        return this;
    }

    /**
     * Set the identifierColumnHeader to {@code name}.
     * 
     * @param name
     * @return this
     * @deprecated use {@link #setIdentifierColumnHeader(String)} instead
     */
    @Deprecated
    public PrettyLinkedTableMap<R, C, V> setRowName(String name) {
        return setIdentifierColumnHeader(name);
    }

    @Override
    public String toString() {
        int total = 0;
        Object[] header = new Object[columns.size() + 1];
        header[0] = identifierColumnHeader;
        String format = "| %-" + identifierColumnLength + "s | ";
        int i = 1;
        for (java.util.Map.Entry<C, Integer> entry : columns.entrySet()) {
            format += "%-" + entry.getValue() + "s | ";
            total += entry.getValue() + 3;
            header[i] = entry.getKey();
            ++i;
        }
        format += "%n";
        String hr = Strings.padEnd("+", identifierColumnLength + 3 + total,
                '-');
        hr += "+" + System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        sb.append(System.getProperty("line.separator"));
        sb.append(hr);
        sb.append(String.format(format, header));
        sb.append(hr);
        for (R row : keySet()) {
            Object[] rowdata = new Object[columns.size() + 1];
            rowdata[0] = row;
            i = 1;
            for (C column : columns.keySet()) {
                rowdata[i] = get(row).get(column);
                ++i;
            }
            sb.append(String.format(format, rowdata));
        }
        sb.append(hr);
        return sb.toString();
    }

    /**
     * A wrapper around a "table" map that generates a
     * {@link PrettyLinkedTableMap} for its data on the fly, when necessary
     * (e.g. {@link #toString()} and any attempts to mutate the original
     * source).
     *
     * @author Jeff Nelson
     */
    private static class LazyPrettyLinkedTableMap<R, C, V>
            extends ForwardingMap<R, Map<C, V>> implements
            PrettyTableMap<R, C, V> {

        /**
         * The header to use for the column that contains the row identifier in
         * the pretty output.
         */
        private final String identifierColumnHeader;

        /**
         * The existing source that it passed in to be {@link #toString()
         * prettified} later.
         */
        private Map<R, Map<C, V>> source;

        /**
         * Construct a new instance.
         * 
         * @param source
         * @param identifierColumnHeader
         */
        private LazyPrettyLinkedTableMap(Map<R, Map<C, V>> source,
                String identifierColumnHeader) {
            this.source = source;
            this.identifierColumnHeader = identifierColumnHeader;
        }

        @Override
        public void clear() {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            pretty().clear();
        }

        @Override
        public V put(R row, C column, V value) {
            return ((PrettyTableMap<R, C, V>) pretty()).put(row, column, value);
        }

        @Override
        public Map<C, V> put(R key, Map<C, V> value) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            return pretty().put(key, value);
        }

        @Override
        public void putAll(Map<? extends R, ? extends Map<C, V>> map) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            pretty().putAll(map);
        }

        @Override
        public Map<C, V> remove(Object object) {
            // Force generation of pretty map and use it as the #source so the
            // original map isn't mutated
            return pretty().remove(object);
        }

        @Override
        public PrettyTableMap<R, C, V> setIdentifierColumnHeader(
                String header) {
            return ((PrettyTableMap<R, C, V>) pretty())
                    .setIdentifierColumnHeader(header);
        }

        @Override
        public String toString() {
            return pretty().toString();
        }

        @Override
        protected Map<R, Map<C, V>> delegate() {
            return source;
        }

        /**
         * Return a {@link Map} with all the contained data that is guaranteed
         * to be pretty.
         * 
         * @return a pretty map
         */
        private Map<R, Map<C, V>> pretty() {
            if(!(source instanceof PrettyLinkedTableMap)) {
                Map<R, Map<C, V>> staging = new PrettyLinkedTableMap<>(
                        identifierColumnHeader);
                staging.putAll(source);
                source = staging;
            }
            return source;
        }

    }
}
