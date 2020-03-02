/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
