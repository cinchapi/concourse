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
package com.cinchapi.concourse.exporter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.cli.CommandLineInterface;

public final class ExportCli extends CommandLineInterface {
    private final ExportOptions options;

    public ExportCli(String[] args) {
        super(args);
        this.options = (ExportOptions) super.options;
    }

    @Override
    protected void doTask() {
        System.out.println(options.hidePrimaryKey);
        output(getRecords(), getOutputStream());
    }

    @Override
    protected ExportOptions getOptions() {
        return new ExportOptions();
    }

    private Iterable<Map<String, Set<Object>>> getRecords() {
        return Helper.map(getKeyedRecords(), (id, xs) ->
            Helper.mapToMap(xs, (k, v) -> Helper.tuple(
                    !options.hidePrimaryKey ? id.toString() + "," + k : k, v)));
    }

    /*
     * FUTURE NOTE: If https://openjdk.java.net/jeps/8213076 gets through,
     * and a standard Tuple/Pair, or we construct one, then this could could be
     * quite a bit shorter.
     */
    private Map<Long, Map<String, Set<Object>>> getKeyedRecords() {
        if(options.criteria != null && options.records.size() > 0) {
            return Helper.filter(concourse.select(options.criteria),
                    (k, v) -> options.records.contains(k));
        }
        else if(options.criteria != null) {
            return concourse.select(options.criteria);
        }
        else if(options.records.size() > 0) {
            return concourse.select(options.records);
        }
        else {
            return concourse.select(concourse.inventory());
        }
    }

    private OutputStream getOutputStream() {
        try {
            final Path path = createFile(options.fileName);
            return path == null ? System.out : Files.newOutputStream(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create file's stream");
        }
    }

    private Path createFile(String fileName) {
        try {
            final Path path = getPathOrNull(fileName);
            return path == null ? null : Files.createFile(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to interact with stdin.");
        }
    }

    private Path getPathOrNull(String path) {
        try {
            return Paths.get(path);
        }
        catch (InvalidPathException | NullPointerException e) {
            return null;
        }
    }

    private static <T, Q> void output(Iterable<Map<T, Q>> items,
            OutputStream output) {
        PrintStream printer = new PrintStream(output);

        for (Map<T, Q> item : items) {
            for (Map.Entry<T, Q> entry : item.entrySet()) {
                outputItem(printer, entry.getKey(), entry.getValue());
            }
        }
    }

    private static <T, Q> void outputItem(PrintStream printer, T k, Q v) {
        if(v instanceof String) {
            printer.println(k + ","
                    + AnyStrings.ensureWithinQuotesIfNeeded((String) v, ','));
        }
        printer.println(k + "," + v);
    }

}
