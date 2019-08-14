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
package com.cinchapi.concourse.exporter.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.cli.CommandLineInterface;
import com.cinchapi.concourse.cli.Options;
import com.cinchapi.concourse.exporter.Exporter;
import com.cinchapi.concourse.exporter.Exporters;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * A CLI that uses the export framework to export data from Concourse to an
 * output stream.
 *
 * @author Jeff Nelson
 */
public final class ExportCli extends CommandLineInterface {

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public ExportCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask() {
        // TODO: depends on common parser for Order, Page
        // TODO: depends on common converter for comma separated strings to
        // Collection
        ExportOptions opts = (ExportOptions) options;
        Path file = null;
        if(!Empty.ness().describes(opts.file)) {
            file = Paths.get(opts.file);
            File $file = file.toFile();
            $file.mkdirs();
            try {
                $file.createNewFile();
            }
            catch (IOException e) {
                CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        Map<Long, Map<String, Set<Object>>> data;
        // TODO: what about order and pagination?
        if(!Empty.ness().describes(opts.ccl)
                && !Empty.ness().describes(opts.keys)) {
            data = concourse.select(opts.keys, opts.ccl);
        }
        else if(!Empty.ness().describes(opts.ccl)) {
            data = concourse.select(opts.ccl);
        }
        else {
            data = concourse.select(concourse.inventory());
        }
        Exporter<Set<Object>> exporter = file != null ? Exporters.csv(file)
                : Exporters.csv();
        if(!opts.excludeRecordId) {
            data.forEach((id, object) -> {
                object.put("id", ImmutableSet.of(id));
            });
        }
        exporter.export(data.values());
    }

    @Override
    protected ExportOptions getOptions() {
        return new ExportOptions();
    }

    /**
     * Export specific {@link Options}.
     * 
     * @author jnelson
     */
    private static class ExportOptions extends Options {
        @Parameter(names = { "-r",
                "--records" }, description = "Comma separated list of records to export")
        public Set<Long> records = Sets.newLinkedHashSet();

        @Parameter(names = { "-c",
                "-ccl" }, description = "Criteria statement (in CCL) that describes how to find the records to export")
        public String ccl = null;

        @Parameter(names = { "-f",
                "--file" }, description = "File to export data to.")
        public String file = null;

        @Parameter(names = "--exclude-record-id", description = "Flag to not display the primary key when exporting.")
        public boolean excludeRecordId = false;

        @Parameter(names = { "-k",
                "--keys" }, description = "Comma separated list of keys to select from each record. By default, all of a record's keys are selected")
        public Set<String> keys = Sets.newHashSet();
    }

}
