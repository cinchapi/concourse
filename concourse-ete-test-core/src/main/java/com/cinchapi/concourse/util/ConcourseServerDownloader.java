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
package com.cinchapi.concourse.util;

import java.nio.file.Paths;

import com.cinchapi.concourse.automation.developer.ConcourseArtifacts;

/**
 * A utility class that can download Concourse Server binaries from Github.
 * 
 * @author jnelson
 * @deprecated use
 *             {@link com.cinchapi.concourse.automation.developer.ConcourseArtifacts}
 *             instead
 */
@Deprecated
public final class ConcourseServerDownloader {

    /**
     * Download the Concourse Server binary of the specified {@code version} to
     * the user's home directory.
     * 
     * @param version
     * @return the absolute path to the downloaded file
     */
    public static String download(String version) {
        return ConcourseArtifacts.installer(version).toString();
    }

    /**
     * Download the Concourse Server binary of the specified {@code version} to
     * the
     * specified {@code location}.
     * 
     * @param version
     * @param location
     * @return the absolute path to the downloaded file
     */
    public static String download(String version, String location) {
        return ConcourseArtifacts.installer(version, Paths.get(location))
                .toString();
    }

    private ConcourseServerDownloader() {}

}
