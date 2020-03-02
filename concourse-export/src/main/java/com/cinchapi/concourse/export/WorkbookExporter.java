/*
 * Cinchapi Inc. CONFIDENTIAL
 * Copyright (c) 2020 Cinchapi Inc. All Rights Reserved.
 *
 * All information contained herein is, and remains the property of Cinchapi.
 * The intellectual and technical concepts contained herein are proprietary to
 * Cinchapi and may be covered by U.S. and Foreign Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained from Cinchapi. Access to the source code
 * contained herein is hereby forbidden to anyone except current Cinchapi
 * employees, managers or contractors who have executed Confidentiality and
 * Non-disclosure agreements explicitly covering such access.
 *
 * The copyright notice above does not evidence any actual or intended
 * publication or disclosure of this source code, which includes information
 * that is confidential and/or proprietary, and is a trade secret, of Cinchapi.
 *
 * ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC PERFORMANCE, OR PUBLIC
 * DISPLAY OF OR THROUGH USE OF THIS SOURCE CODE WITHOUT THE EXPRESS WRITTEN
 * CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
 * LAWS AND INTERNATIONAL TREATIES. THE RECEIPT OR POSSESSION OF THIS SOURCE
 * CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS TO
 * REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR
 * SELL ANYTHING THAT IT MAY DESCRIBE, IN WHOLE OR IN PART.
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
        int index = 0;
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
