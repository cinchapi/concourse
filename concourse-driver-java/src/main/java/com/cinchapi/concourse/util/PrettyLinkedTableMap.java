/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
public class PrettyLinkedTableMap<R, C, V> extends LinkedHashMap<R, Map<C, V>> {

    /**
     * Return an empty {@link PrettyLinkedTableMap} with the default row name.
     * 
     * @return the PrettyLinkedTableMap
     */
    public static <R, C, V> PrettyLinkedTableMap<R, C, V> newPrettyLinkedTableMap() {
        return new PrettyLinkedTableMap<R, C, V>(null);
    }

    /**
     * Return an empty {@link PrettyLinkedTableMap} with the specified
     * {@code rowName}.
     * 
     * @param rowName
     * @return the PrettyLinkedTableMap
     */
    public static <R, C, V> PrettyLinkedTableMap<R, C, V> newPrettyLinkedTableMap(
            String rowName) {
        return new PrettyLinkedTableMap<R, C, V>(rowName);
    }

    private String rowName = "Row";
    private int rowLength = rowName.length();

    /**
     * A mapping from column to display length, which is equal to the greater of
     * the length of the largest value that will be displayed in the column and
     * the value of the column's toString() value.
     */
    private final Map<C, Integer> columns = Maps.newLinkedHashMap();

    /**
     * Construct a new instance.
     * 
     * @param rowName
     */
    private PrettyLinkedTableMap(@Nullable String rowName) {
        if(!Strings.isNullOrEmpty(rowName)) {
            setRowName(rowName);
        }
    }

    /**
     * Insert {@code value} under {@code column} in {@code row}.
     * 
     * @param row
     * @param column
     * @param value
     * @return the previous value located at the intersection of {@code row} and
     *         {@code column} or {@code null} if one did not previously exist.
     */
    public V put(R row, C column, V value) {
        Map<C, V> rowdata = super.get(row);
        if(rowdata == null) {
            rowdata = Maps.newLinkedHashMap();
            super.put(row, rowdata);
        }
        rowLength = Math.max(row.toString().length(), rowLength);
        int current = columns.containsKey(column) ? columns.get(column) : 0;
        columns.put(column,
                Math.max(current, Math.max(column.toString().length(), value
                        .toString().length())));
        return rowdata.put(column, value);
    }

    /**
     * <p>
     * THIS METHOD ALWAYS RETURNS {@code NULL}.
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

    /**
     * Set the rowName to {@code name}.
     * 
     * @param name
     * @return this
     */
    public PrettyLinkedTableMap<R, C, V> setRowName(String name) {
        rowName = name;
        rowLength = Math.max(name.length(), rowLength);
        return this;
    }

    @Override
    public String toString() {
        int total = 0;
        Object[] header = new Object[columns.size() + 1];
        header[0] = rowName;
        String format = "| %-" + rowLength + "s | ";
        int i = 1;
        for (java.util.Map.Entry<C, Integer> entry : columns.entrySet()) {
            format += "%-" + entry.getValue() + "s | ";
            total += entry.getValue() + 3;
            header[i] = entry.getKey();
            ++i;
        }
        format += "%n";
        String hr = Strings.padEnd("+", rowLength + 3 + total, '-');
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
}
