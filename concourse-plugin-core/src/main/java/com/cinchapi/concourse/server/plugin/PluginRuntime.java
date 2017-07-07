/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Every Plugin has an instance of {@link PluginRuntime} that can be used to
 * retrieve information about the plugin's execution environment.
 * 
 * @author Jeff Nelson
 */
public final class PluginRuntime {

    /**
     * Singleton instance.
     */
    public static final PluginRuntime INSTANCE = new PluginRuntime();

    /**
     * Return the {@link PluginRuntime} instance.
     * 
     * @return the PluginRuntime
     */
    public static PluginRuntime getRuntime() {
        return INSTANCE;
    }

    /**
     * Return the location of the {@link Plugin plugin's} home directory.
     * 
     * @return the plugin's home directory
     */
    public Path home() {
        String home = System.getProperty(Plugin.PLUGIN_HOME_JVM_PROPERTY);
        if(home == null) {
            // This usually means that the Plugin is running from an IDE or a
            // unit test, so lets set the home to the working directory
            home = System.getProperty("user.dir");
        }
        return Paths.get(home).toAbsolutePath();
    }

    /**
     * Return the location of the {@link Plugin plugin's} data directory.
     * 
     * @return the plugin's data directory
     */
    public Path data() {
        Path path = home().resolve("data");
        Path devPath = home().resolve("data.dev");
        return devPath.toFile().exists() ? devPath : path;
    }

    private PluginRuntime() {/* no-op */}

}
