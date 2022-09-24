/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.syntax.OrderTree;
import com.cinchapi.ccl.syntax.PageTree;
import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.cli.ConcourseCommandLineInterface;
import com.cinchapi.concourse.cli.ConcourseOptions;
import com.cinchapi.concourse.export.Exporter;
import com.cinchapi.concourse.export.Exporters;
import com.cinchapi.concourse.lang.ConcourseCompiler;
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
public final class ExportCli extends ConcourseCommandLineInterface {

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
     * The condition that determines which records to export.
     */
    @Nullable
    protected String condition = null;

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
            $file.getParentFile().mkdirs();
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

        // order
        if(!Empty.ness().describes(opts.order)) {
            if(!opts.order.toLowerCase().startsWith("order by")) {
                opts.order = "ORDER BY " + opts.order;
            }
            try {
                AbstractSyntaxTree ast = ConcourseCompiler.get()
                        .parse(opts.order);
                OrderTree tree = (OrderTree) ast;
                order = Order.from(tree);
            }
            catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid order CCL statement");
            }
        }

        // page
        String $page = "";
        if(!Empty.ness().describes(opts.page)) {
            $page += "PAGE " + opts.page + " ";
        }
        if(!Empty.ness().describes(opts.size)) {
            $page += "SIZE " + opts.size;
        }
        if(!Empty.ness().describes($page)) {
            try {
                AbstractSyntaxTree ast = ConcourseCompiler.get().parse($page);
                PageTree tree = (PageTree) ast;
                page = Page.from(tree);
            }
            catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid page or size argument");
            }
        }

        excludeRecordId = opts.excludeRecordId;
        condition = opts.condition;
    }

    @Override
    protected void doTask() {
        Map<Long, Map<String, Set<Object>>> data;
        if(!Empty.ness().describes(records) && !Empty.ness().describes(keys)
                && !Empty.ness().describes(order)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, records, order, page);
        }
        else if(!Empty.ness().describes(condition)
                && !Empty.ness().describes(keys)
                && !Empty.ness().describes(order)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, condition, order, page);
        }
        else if(!Empty.ness().describes(records)
                && !Empty.ness().describes(keys)
                && !Empty.ness().describes(order)) {
            data = concourse.select(keys, records, order);
        }
        else if(!Empty.ness().describes(records)
                && !Empty.ness().describes(keys)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, records, page);
        }
        else if(!Empty.ness().describes(condition)
                && !Empty.ness().describes(keys)
                && !Empty.ness().describes(order)) {
            data = concourse.select(keys, condition, order);
        }
        else if(!Empty.ness().describes(records)
                && !Empty.ness().describes(page)
                && !Empty.ness().describes(order)) {
            data = concourse.select(records, page, order);
        }
        else if(!Empty.ness().describes(page) && !Empty.ness().describes(order)
                && !Empty.ness().describes(condition)) {
            data = concourse.select(condition, page, order);
        }
        else if(!Empty.ness().describes(condition)
                && !Empty.ness().describes(keys)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, condition, page);
        }
        else if(!Empty.ness().describes(keys) && !Empty.ness().describes(order)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, concourse.inventory(), order, page);
        }
        else if(!Empty.ness().describes(records)
                && !Empty.ness().describes(keys)) {
            data = concourse.select(keys, records);
        }
        else if(!Empty.ness().describes(condition)
                && !Empty.ness().describes(keys)) {
            data = concourse.select(keys, condition);
        }
        else if(!Empty.ness().describes(page)
                && !Empty.ness().describes(records)) {
            data = concourse.select(records, page);
        }
        else if(!Empty.ness().describes(order)
                && !Empty.ness().describes(records)) {
            data = concourse.select(records, order);
        }
        else if(!Empty.ness().describes(page)
                && !Empty.ness().describes(condition)) {
            data = concourse.select(condition, page);
        }
        else if(!Empty.ness().describes(order)
                && !Empty.ness().describes(condition)) {
            data = concourse.select(condition, order);
        }
        else if(!Empty.ness().describes(keys)
                && !Empty.ness().describes(order)) {
            data = concourse.select(keys, concourse.inventory(), order);
        }
        else if(!Empty.ness().describes(keys)
                && !Empty.ness().describes(page)) {
            data = concourse.select(keys, concourse.inventory(), page);
        }
        else if(!Empty.ness().describes(order)
                && !Empty.ness().describes(page)) {
            data = concourse.select(concourse.inventory(), order, page);
        }
        else if(!Empty.ness().describes(records)) {
            data = concourse.select(records);
        }
        else if(!Empty.ness().describes(condition)) {
            data = concourse.select(condition);
        }
        else if(!Empty.ness().describes(order)) {
            data = concourse.select(concourse.inventory(), order);
        }
        else if(!Empty.ness().describes(page)) {
            data = concourse.select(concourse.inventory(), page);
        }
        else if(!Empty.ness().describes(keys)) {
            data = concourse.select(keys, concourse.inventory());
        }
        else {
            data = concourse.select(concourse.inventory());
        }
        Exporter<Set<Object>> exporter = file != null ? Exporters.csv(file)
                : Exporters.csv(); // TODO: add support for workbooks?
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
     * Export specific {@link ConcourseOptions}.
     * 
     * @author jnelson
     */
    private static class ExportOptions extends ConcourseOptions {
        @Parameter(names = { "-r",
                "--records" }, description = "Comma separated list of records to export", variableArity = true)
        public List<String> records = Lists.newArrayList();

        @Parameter(names = { "--condition",
                "--where" }, description = "CCL condition statement that describes how to find the records to export")
        public String condition = null;

        @Parameter(names = { "-o",
                "--order" }, description = "CCL order statement that describes how to sort the exported data")
        public String order = null;

        @Parameter(names = {
                "--page" }, description = "The data page to export (default: the first page)")
        public Integer page = null;

        @Parameter(names = {
                "--size" }, description = "The maximum number of records to include on the exported page (default: export all data)")
        public Integer size = null;

        @Parameter(names = { "-f",
                "--file" }, description = "File to which the data should be exported.")
        public String file = null;

        @Parameter(names = "--exclude-record-id", description = "Flag to not display the primary key when exporting.")
        public boolean excludeRecordId = false;

        @Parameter(names = { "-k", "--keys",
                "--select" }, description = "Comma separated list of keys to select from each record. By default, all of a record's keys are selected", variableArity = true)
        public List<String> keys = Lists.newArrayList();
    }

}
