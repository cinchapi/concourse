/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.importer.util;

import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * File utilities that are used in the import classes.
 * <p>
 * <strong>NOTE:</strong> This class is copied from the FileSystem class in
 * concourse-server.
 * </p>
 * 
 * @author Jeff Nelson
 */
public final class Files {

    /**
     * Expand the given {@code path} so that it contains completely normalized
     * components (e.g. ".", "..", and "~" are resolved to the correct absolute
     * paths).
     * 
     * @param path
     * @return the expanded path
     */
    public static String expandPath(String path) {
        path = path.replaceAll("~", USER_HOME);
        return BASE_PATH.resolve(path).normalize().toString();
    }

    /**
     * The user's home directory, which is used to expand path names with "~"
     * (tilde).
     */
    private static String USER_HOME = System.getProperty("user.home");

    /**
     * The working directory from which the current JVM process was launched.
     */
    private static String WORKING_DIRECTORY = System.getProperty("user.dir");

    /**
     * The base path that is used to resolve and normalize other relative paths.
     */
    private static Path BASE_PATH = FileSystems.getDefault().getPath(
            WORKING_DIRECTORY);

    private Files() {/* noop */}

}
