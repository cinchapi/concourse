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

import com.cinchapi.bucket.Bucket;

/**
 * An interface that provides some default implementations for facilities that
 * provide information about the state of a {@link Plugin}.
 * 
 * @author Jeff Nelson
 */
public interface PluginStateContainer {

    /**
     * Get the directory where the plugin store's data.
     * 
     * @return the data directory
     */
    public default Path data() {
        Path path = home().resolve("data");
        Path devPath = home().resolve("data.dev");
        return devPath.toFile().exists() ? devPath : path;
    }

    /**
     * Get the plugin's home/working directory.
     * 
     * @return the home directory
     */
    public Path home();

    /**
     * Return a {@link Bucket} that can be used to provide local storage under
     * the given {@code namespace}.
     * 
     * @param namespace the namespace to use for the local storage
     * @return a {@link Bucket} for local storage
     */
    public default Bucket localStorage(String namespace) {
        Path file = data().resolve("localStorage.bkt");
        return Bucket.connect(file, namespace);
    }

    /**
     * Return a {@link Bucket} that can be used to provide general local
     * storage.
     * 
     * @return a {@link Bucket} for general local storage
     */
    public default Bucket localStorage() {
        return localStorage("general");
    }

}
