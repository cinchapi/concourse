/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;

/**
 * A set of Options for a CLI that interact with a server environment.
 * 
 * @author Jeff Nelson
 */
public class EnvironmentManagementOptions extends ManagementOptions {

    @Parameter(names = { "-e",
            "--environment" }, description = "The environment of the Concourse Server to use")
    public String environment = "";

    /**
     * Return a description of the {@link #environment} option. For example, if
     * no environment is specified, the description will indicate that the
     * server's configuration of a default environment will be used.
     * 
     * @return a description of the environment
     */
    public String environmentDescription() {
        return Strings.isNullOrEmpty(environment)
                ? "the default environment as defined in Concourse Server's configuration"
                : "the '" + environment + "' environment";
    }

}
