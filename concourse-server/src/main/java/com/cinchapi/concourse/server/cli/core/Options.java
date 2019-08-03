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
package com.cinchapi.concourse.server.cli.core;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.google.common.collect.Lists;

/**
 * Each member variable represents the options that can be passed to the main
 * method of a CLI. Each CLI should subclass this and specify the appropriate
 * parameters.
 * 
 * <p>
 * See http://jcommander.org/ for more information.
 * <p>
 * 
 * @author Jeff Nelson
 */
public abstract class Options {

    /**
     * A handler for the client preferences that <em>may</em> exist in the
     * user's home directory. If the file is available, its contents will be
     * used for configuration defaults.
     */
    private ConcourseClientPreferences defaults = ConcourseClientPreferences
            .fromUserHomeDirectory();

    @Parameter(names = { "-h", "--help" }, help = true, hidden = true)
    public boolean help;

    @Parameter(names = { "-u",
            "--username" }, description = "The username with which to connect")
    public String username = defaults.getUsername();

    @Parameter(names = "--password", description = "The password", hidden = true)
    public String password = new String(defaults.getPasswordExplicit());

    /**
     * Contains all the non parameterized arguments that are passed to the
     * program. This is typically what would be available in the array passed to
     * Java's main method.
     */
    @Parameter(description = "additional program arguments...")
    public List<String> args = Lists.newArrayList();

}
