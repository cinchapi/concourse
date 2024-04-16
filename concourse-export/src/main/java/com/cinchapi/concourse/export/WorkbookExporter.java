/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.export;

import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * An {@link Exporter} for "workbook" output.
 * <p>
 * A Workbook is a file that contains multiple sheets of data (i.e. an excel
 * workbook)
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class WorkbookExporter<V> extends Exporter<V> {

    /**
     * Construct a new instance.
     * 
     * @param output
     */
    public WorkbookExporter(OutputStream output) {
        super(output);
    }

    /**
     * Construct a new instance.
     * 
     * @param output
     * @param toStringFunction
     */
    public WorkbookExporter(OutputStream output,
            Function<V, String> toStringFunction) {
        super(output, toStringFunction);
    }

    @Override
    public final void export(Iterable<Map<String, V>> data) {
        exportAll(ImmutableList.of(data));
    }

    /**
     * {@link #export(Iterable) Export} each inner iterable within the
     * {@code data} as
     * a sheet in the workbook.
     * 
     * @param data
     */
    public final void exportAll(Iterable<Iterable<Map<String, V>>> data) {
        Map<String, Iterable<Map<String, V>>> $data = Maps.newLinkedHashMap();
        int index = 1;
        for (Iterable<Map<String, V>> sheet : data) {
            $data.put("Sheet " + index, sheet);
        }
        exportAll($data);
    }

    /**
     * {@link #export(Iterable) Export} each inner iterable within the
     * {@code data} as
     * a sheet in the workbook.
     * 
     * @param data a mapping from the name of each sheet to the respective data
     *            that should be included
     */
    public abstract void exportAll(Map<String, Iterable<Map<String, V>>> data);

}
