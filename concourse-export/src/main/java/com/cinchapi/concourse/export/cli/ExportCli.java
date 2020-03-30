/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.export.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.beust.jcommander.Parameter;
import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.cli.CommandLineInterface;
import com.cinchapi.concourse.cli.Options;
import com.cinchapi.concourse.export.Exporter;
import com.cinchapi.concourse.export.Exporters;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * A CLI that uses the export framework to export data from Concourse to an
 * output stream.
 *
 * @author Jeff Nelson
 */
public final class ExportCli extends CommandLineInterface {

    /**
     * The set of records from which to export data.
     */
    @Nullable
    protected Set<Long> records = null;

    /**
     * The file to which the exported data is stored.
     */
    @Nullable
    protected Path file = null;

    /**
     * The criteria that determines which records to export.
     */
    @Nullable
    protected String ccl = null;

    /**
     * The order of the exported data.
     */
    @Nullable
    protected Order order = null;

    /**
     * The page of the data to export.
     */
    @Nullable
    protected Page page = null;

    /**
     * The set of keys to include in the data export for each exported record.
     */
    @Nullable
    protected Set<String> keys = null;

    /**
     * A flag that determines of the record id should be included with the
     * exported data.
     */
    protected boolean excludeRecordId = getOptions().excludeRecordId;

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public ExportCli(String[] args) {
        super(args);
        init();
    }

    /**
     * Initialize the CLI based on the arguments that are provided.
     */
    private void init() {
        ExportOptions opts = (ExportOptions) options;

        // file
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

        // records
        if(!Empty.ness().describes(opts.records)) {
            records = AnyObjects.split(opts.records, ',').stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        // keys
        if(!Empty.ness().describes(opts.keys)) {
            keys = AnyObjects.split(opts.keys, ',').stream()
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        // TODO: depends on common parser for Order, Page
        excludeRecordId = opts.excludeRecordId;

    }

    @Override
    protected void doTask() {
        Map<Long, Map<String, Set<Object>>> data;
        if(!Empty.ness().describes(ccl) && !Empty.ness().describes(keys)) {
            data = concourse.select(keys, ccl);
        }
        else if(!Empty.ness().describes(ccl)) {
            data = concourse.select(ccl);
        }
        else {
            data = concourse.select(concourse.inventory());
        }
        Exporter<Set<Object>> exporter = file != null ? Exporters.csv(file)
                : Exporters.csv();
        if(!excludeRecordId) {
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
                "--records" }, description = "Comma separated list of records to export", variableArity = true)
        public List<String> records = Lists.newArrayList();

        @Parameter(names = { "-c",
                "-ccl" }, description = "Criteria statement (in CCL) that describes how to find the records to export")
        public String ccl = null;

        @Parameter(names = { "-f",
                "--file" }, description = "File to export data to.")
        public String file = null;

        @Parameter(names = "--exclude-record-id", description = "Flag to not display the primary key when exporting.")
        public boolean excludeRecordId = false;

        @Parameter(names = { "-k",
                "--keys" }, description = "Comma separated list of keys to select from each record. By default, all of a record's keys are selected", variableArity = true)
        public List<String> keys = Lists.newArrayList();
    }

}
