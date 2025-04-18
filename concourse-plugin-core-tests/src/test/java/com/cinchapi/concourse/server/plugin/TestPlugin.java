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
package com.cinchapi.concourse.server.plugin;

import java.util.Set;

import ch.qos.logback.classic.Level;

import com.cinchapi.common.reflect.Reflection;

/**
 * A plugin to be used in unit tests.
 * 
 * @author Jeff Nelson
 */
public class TestPlugin extends Plugin {

    /**
     * Construct a new instance.
     * 
     * @param fromServer
     * @param fromPlugin
     */
    public TestPlugin(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }

    public String echo(String input) {
        return input;
    }

    public Set<Long> inventory() {
        return runtime.inventory();
    }

    public String getResultDatasetClassName() {
        return runtime.selectCcl("foo = bar").getClass().getName();
    }

    public String environment() {
        return Reflection.call(Thread.currentThread(), "environment");
    }

    @Override
    protected PluginConfiguration getConfig() {
        return new StandardPluginConfiguration() {

            @Override
            public Level getLogLevel() {
                return Level.DEBUG;
            }

        };
    }

}
