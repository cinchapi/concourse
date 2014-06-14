/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * A special {@link LinkedHashMap} that holds data mapping a row key to a
 * submapping for column key to value (e.g. Map[R, -> Map[C -> V]]). This object
 * has pretty {@link #toString()} output and
 * 
 * @author jnelson
 */
@SuppressWarnings("serial")
public class TLinkedTableMap<R, C, V> extends LinkedHashMap<R, Map<C, V>> {

    /**
     * Return an empty {@link TLinkedTableMap} with the default row name.
     * 
     * @return the TLinkedTableMap
     */
    public static <R, C, V> TLinkedTableMap<R, C, V> newTLinkedTableMap() {
        return new TLinkedTableMap<R, C, V>(null);
    }

    /**
     * Return an empty {@link TLinkedTableMap} with the specified
     * {@code rowName}.
     * 
     * @param rowName
     * @return the TLinkedTableMap
     */
    public static <R, C, V> TLinkedTableMap<R, C, V> newTLinkedTableMap(
            String rowName) {
        return new TLinkedTableMap<R, C, V>(rowName);
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
    private TLinkedTableMap(@Nullable String rowName) {
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
    public TLinkedTableMap<R, C, V> setRowName(String name) {
        rowName = name;
        rowLength = Math.max(name.length(), rowLength);
        return this;
    }

    @Override
    public String toString() {
        int total = 0;
        Object[] header = new String[columns.size() + 1];
        header[0] = rowName;
        String format = "| %-" + rowLength + "s | ";
        int i = 1;
        for (java.util.Map.Entry<C, Integer> entry : columns.entrySet()) {
            format += "%-" + entry.getValue() + "s | ";
            total += entry.getValue() + 3;
            header[i] = entry.getKey();
            i++;
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
                i++;
            }
            sb.append(String.format(format, rowdata));
        }
        sb.append(hr);
        return sb.toString();
    }
}
