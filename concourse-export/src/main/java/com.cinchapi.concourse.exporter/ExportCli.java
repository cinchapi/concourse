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

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.cli.CommandLineInterface;
import com.cinchapi.concourse.exporter.helpers.NullHelper;
import com.cinchapi.concourse.exporter.interactors.Exporter;

public final class ExportCli extends CommandLineInterface {
    private final ExportOptions opts;

    public ExportCli(String[] args) {
        super(args);
        this.opts = (ExportOptions) super.options;
    }

    @Override
    protected void doTask() {
        final Exporter exporter = new Exporter(concourse, !opts.hidePrimaryKey);

        /*
         * NOTE: If https://openjdk.java.net/jeps/8213076 gets through,
         * and a standard Tuple/Pair, or we construct one,
         * then this could could be quite a bit shorter.
         *
         * NOTE 2: `result` is effectively final, Java just lacks if/else
         * expressions, as well as any sort of pattern matching.
         * If in the future Java gets either of the above, make this final.
         */
        String result;
        if(opts.criteria != null && opts.records.size() > 0) {
            result = exporter.perform(opts.records, opts.criteria);
        }
        else if(opts.criteria != null) {
            result = exporter.perform(opts.criteria);
        }
        else if(opts.records.size() > 0) {
            result = exporter.perform(opts.records);
        }
        else {
            result = exporter.perform();
        }

        final OutputStream outputStream = getOutputStream();
        final PrintStream printStream = new PrintStream(outputStream);

        printStream.println(result);
    }

    @Override
    protected ExportOptions getOptions() {
        return new ExportOptions();
    }

    private OutputStream getOutputStream() {
        return NullHelper.orElse(
                NullHelper.$try(() -> Files.newOutputStream(
                        Files.createFile(Paths.get(opts.fileName)))),
                System.out);
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
