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
package com.cinchapi.concourse.server.cli.plugin;

import com.cinchapi.concourse.server.cli.core.ManagementCommandLineInterface;
import com.cinchapi.concourse.server.cli.core.ManagementOptions;

/**
 * Entry point for management CLIs to add/remove/upgrade/etc plugins.
 * 
 * @author Jeff Nelson
 */
public abstract class PluginCli extends ManagementCommandLineInterface {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public PluginCli(String[] args) {
        super(args);
    }

    @Override
    protected ManagementOptions getOptions() {
        return new ManagementOptions();
    }

}
