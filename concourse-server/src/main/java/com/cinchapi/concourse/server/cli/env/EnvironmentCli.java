/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli.env;

import com.cinchapi.concourse.server.cli.core.ManagementCommandLineInterface;
import com.cinchapi.concourse.server.cli.core.ManagementOptions;

/**
 * Entry point for management CLIs that add/modify/remove Concourse Server
 * environments.
 * 
 * @author Jeff Nelson
 */
public abstract class EnvironmentCli extends ManagementCommandLineInterface {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public EnvironmentCli(String[] args) {
        super(args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> Do not use
     * {@link com.cinchapi.concourse.server.cli.core.EnvironmentManagementOptions
     * EnvironmentManagementOptions} here because the target environment for any
     * management actions that are executed against it (e.g., delete) should be
     * specified as an arg instead of an parameter.
     * </p>
     */
    @Override
    protected ManagementOptions getOptions() {
        return new ManagementOptions();
    }

}
