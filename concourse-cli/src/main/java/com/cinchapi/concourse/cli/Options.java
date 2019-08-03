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
package com.cinchapi.concourse.cli;

import java.util.List;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Each member variable represents the options that can be passed to the main
 * method of a CLI. Each CLI should (anonymously) subclass this and specify the
 * appropriate parameters.
 * 
 * <p>
 * See http://jcommander.org/ for more information.
 * <p>
 * 
 * @author Jeff Nelson
 */
public class Options {

    /**
     * A handler for the client preferences that <em>may</em> exist in the
     * user's home directory. If the file is available, its contents will be
     * used for configuration defaults.
     */
    private ConcourseClientPreferences defaults = ConcourseClientPreferences
            .fromUserHomeDirectory();

    @Parameter(names = { "--help" }, help = true, hidden = true)
    public boolean help;

    @Parameter(names = { "-h",
            "--host" }, description = "The hostname where the Concourse Server is located")
    public String host = defaults.getHost();

    @Parameter(names = { "-p",
            "--port" }, description = "The port on which the Concourse Server is listening")
    public int port = defaults.getPort();

    @Parameter(names = { "-u",
            "--username" }, description = "The username with which to connect")
    public String username = defaults.getUsername();

    @Parameter(names = "--password", description = "The password", password = false, hidden = true)
    public String password = new String(defaults.getPasswordExplicit());

    @Parameter(names = { "-e",
            "--environment" }, description = "The environment of the Concourse Server to use")
    public String environment = defaults.getEnvironment();

    @Parameter(names = "--prefs", description = "Path to the concourse_client.prefs file")
    public String prefs;

    @Parameter(names = {
            "--verbose" }, description = "Turn on the display of informational logging")
    public boolean verbose = false;

    @DynamicParameter(names = "-D", description = "Use this flag to define a dynamic paramter")
    public Map<String, String> dynamic = Maps.newHashMap();

    /**
     * Contains all the non parameterized arguments that are passed to the
     * program. This is typically what would be available in the array passed to
     * Java's main method.
     */
    @Parameter(description = "additional program arguments...")
    protected List<String> args = Lists.newArrayList();

}
