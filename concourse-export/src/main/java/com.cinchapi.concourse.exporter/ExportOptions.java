package com.cinchapi.concourse.exporter;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.cli.Options;

import java.util.HashSet;
import java.util.Set;

public class ExportOptions extends Options {
    @Parameter(names = { "-r",  "--records" }, description = "Comma separated list of records to export")
    public Set<Long> records = new HashSet<>();

    @Parameter(names = { "-c", "-ccl" },
            description = "Criteria statement (in CCL) that describes how to find the records to export")
    public String criteria = null;

    @Parameter(names = "--no-primary-key", description="Flag to not display the primary key when exporting.")
    public boolean hidePrimaryKey = false;
}