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

import java.util.HashSet;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.cli.Options;

public class ExportOptions extends Options {
    @Parameter(names = { "-r",
            "--records" }, description = "Comma separated list of records to export")
    public Set<Long> records = new HashSet<>();

    @Parameter(names = { "-c",
            "-ccl" }, description = "Criteria statement (in CCL) that describes how to find the records to export")
    public String criteria = null;

    @Parameter(names = "--no-primary-key", description = "Flag to not display the primary key when exporting.")
    public boolean hidePrimaryKey = false;
}