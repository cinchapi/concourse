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

import java.nio.file.Path;

/**
 * Marker class for the standard plugin configuration.
 *
 * @author Jeff Nelson
 */
public class StandardPluginConfiguration extends PluginConfiguration {

    /**
     * Construct a new instance.
     */
    public StandardPluginConfiguration() {
        super();
    }

    /**
     * Construct a new instance.
     *
     * @param path
     */
    protected StandardPluginConfiguration(Path path) {
        super(path);
    }
}
