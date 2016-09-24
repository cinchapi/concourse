/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;

/**
 * Information about the runtime context in which a {@link Plugin} operates.
 * Provided by Concourse Server, to plugin hooks (i.e.
 * {@link Plugin#afterInstall() afterInstall}).
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class PluginContext {

    /**
     * The plugin's working directory.
     */
    private final Path home;

    /**
     * Construct a new instance.
     * 
     * @param home
     */
    @PackagePrivate
    PluginContext(Path home) {
        this.home = home;
    }

    /**
     * Get the plugin's working directory.
     * 
     * @return the home directory
     */
    public Path home() {
        return home;
    }

    /**
     * Get the directory where the plugin store's data.
     * 
     * @return the data directory
     */
    public Path data() {
        return home.resolve("data").toAbsolutePath();
    }

}
