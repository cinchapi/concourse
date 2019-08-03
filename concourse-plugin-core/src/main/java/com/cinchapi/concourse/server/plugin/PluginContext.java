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
package com.cinchapi.concourse.server.plugin;

import java.nio.file.Path;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.github.zafarkhaja.semver.Version;

/**
 * Information about the runtime context in which a {@link Plugin} operates.
 * Provided by Concourse Server, to plugin hooks (i.e.
 * {@link Plugin#afterInstall() afterInstall}).
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class PluginContext extends PluginStateContainer {

    /**
     * The plugin's working directory.
     */
    private final Path home;

    /**
     * The plugin's version.
     */
    private final Version pluginVersion;

    /**
     * The version of ConcourseServer in which the plugin is installed.
     */
    private final Version concourseVersion;

    /**
     * Construct a new instance.
     * 
     * @param home
     */
    @PackagePrivate
    PluginContext(Path home, String pluginVersion, String concourseVersion) {
        this.home = home;
        this.pluginVersion = Version.valueOf(pluginVersion);
        this.concourseVersion = Version.valueOf(concourseVersion);
    }

    @Override
    public Path home() {
        return home;
    }

    /**
     * Return the plugin's version.
     * 
     * @return the version of the plugin
     */
    public Version pluginVersion() {
        return pluginVersion;
    }

    /**
     * Return Concourse Server's version.
     * 
     * @return the version of Concourse Server
     */
    public Version concourseVersion() {
        return concourseVersion;
    }

}
