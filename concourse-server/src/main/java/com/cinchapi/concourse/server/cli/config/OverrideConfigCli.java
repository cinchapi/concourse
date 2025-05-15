/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli.config;

import jline.console.ConsoleReader;

import com.cinchapi.configctl.lib.ConfigCli;
import com.cinchapi.lib.cli.Options;
import com.cinchapi.lib.config.Configuration;

/**
 *
 *
 * @author Jeff Nelson
 */
public class OverrideConfigCli extends ConfigCli {

    @Override
    protected void setup(Options options, ConsoleReader console) {
        // TODO: do login for Management stuffs
    }

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public OverrideConfigCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask(Configuration config) {
        // TODO: take config and create
        System.out.println("Hey There!");

    }

    @Override
    protected ConfigCliOptions getOptions() {
        return new ValueConfigCliOptions();
    }

}
