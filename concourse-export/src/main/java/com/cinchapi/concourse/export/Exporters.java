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
package com.cinchapi.concourse.export;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.export.excel.ExcelExporter;

/**
 * Factory methods that return various {@link Exporter Exporters}.
 *
 * @author Jeff Nelson
 */
public final class Exporters {

    private Exporters() {/* no-init */}

    /**
     * Open a {@link FileOutputStream} for {@code file} while supressing any
     * checked exceptions (they'll be rethrown as unchecked).
     * 
     * @param file
     * @return the {@link FileOutputStream}
     */
    private static FileOutputStream openFileOutputStreamQuietly(Path file) {
        try {
            return new FileOutputStream(file.toAbsolutePath().toString());
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return an {@link Exporter} that outputs in CSV format.
     * 
     * @return the {@link Exporter}
     */
    public static <V> Exporter<V> csv() {
        return csv(System.out);
    }

    /**
     * Return an {@link Exporter} that outputs in CSV format to the
     * {@code file}.
     * 
     * @param file
     * @return the {@link Exporter}
     */
    public static <V> Exporter<V> csv(Path file) {
        return csv(openFileOutputStreamQuietly(file));
    }

    /**
     * Return the {@link Exporter} that outputs in CSV format to the
     * {@code stream}.
     * 
     * @param stream
     * @return the {@link Exporter}
     */
    public static <V> Exporter<V> csv(OutputStream stream) {
        return new DelimitedLinesExporter<>(stream, ',');
    }

    /**
     * Return the {@link WorkbookExporter} that outputs in Excel format to the
     * {@code file}.
     * 
     * @param file
     * @return the {@link WorkbookExporter}
     */
    public static <V> WorkbookExporter<V> excel(Path file) {
        return excel(openFileOutputStreamQuietly(file));
    }

    /**
     * Return the {@link WorkbookExporter} that outputs in Excel format to the
     * {@code stream}.
     * 
     * @param file
     * @return the {@link WorkbookExporter}
     */
    public static <V> WorkbookExporter<V> excel(OutputStream stream) {
        return new ExcelExporter<>(stream);
    }

}
