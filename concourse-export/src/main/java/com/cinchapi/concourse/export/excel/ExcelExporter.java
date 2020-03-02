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
package com.cinchapi.concourse.export.excel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.export.WorkbookExporter;

/**
 * A {link WorkbokExporter} for generating Microsoft excel files.
 *
 * @author Jeff Nelson
 */
public class ExcelExporter<V> extends WorkbookExporter<V> {

    /**
     * Construct a new instance.
     * 
     * @param output
     */
    public ExcelExporter(OutputStream output) {
        super(output);
    }

    @Override
    public void exportAll(Map<String, Iterable<Map<String, V>>> data) {
        try {
            Workbook workbook = WorkbookFactory.create(true);
            for (Entry<String, Iterable<Map<String, V>>> entry : data
                    .entrySet()) {
                String name = entry.getKey();
                Iterable<Map<String, V>> rows = entry.getValue();
                Sheet sheet = workbook.createSheet(name);
                Row header = sheet.createRow(0);
                int cindex = 0;
                Iterable<String> headers = getHeaders(rows);
                for (String column : headers) {
                    Cell cell = header.createCell(cindex);
                    cell.setCellValue(column);
                    ++cindex;
                }
                int rindex = 1;
                for (Map<String, V> $row : rows) {
                    cindex = 0;
                    Row row = sheet.createRow(rindex);
                    for (String column : headers) {
                        Cell cell = row.createCell(cindex);
                        V value = $row.get(column);
                        if(value != null) {
                            cell.setCellValue(toStringFunction.apply(value));
                        }
                        else {
                            cell.setBlank();
                        }
                        ++cindex;
                    }
                    ++rindex;
                }
            }
            workbook.write(output);
            output.flush();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

}
