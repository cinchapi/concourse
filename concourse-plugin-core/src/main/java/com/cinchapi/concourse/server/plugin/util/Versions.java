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
package com.cinchapi.concourse.server.plugin.util;

import java.util.Arrays;

import com.cinchapi.concourse.util.Strings;
import com.github.zafarkhaja.semver.Version;

/**
 * A utility class for dealing with version numbers.
 * 
 * @author Jeff Nelson
 */
public final class Versions {

    /**
     * Parse the {@code version} string into a well formed semantic
     * {@link Version} object. An exception is thrown if the input does not
     * adhere to proper semver guidelines.
     * 
     * @param version the string to parse
     * @return the semantic version
     */
    public static Version parseSemanticVersion(String version) {
        if(version.matches("\\d+\\.\\d+\\.\\d+\\.\\d+(-[a-zA-Z0-9-]+)?$")) {
            // Cinchapi often departs slightly from semantic versioning
            // by including the build number as the fourth part, immediately
            // after PATCH version (e.g. X.Y.Z.B). That format will fail to
            // parse, so we must modify it to conform strictly to semver.
            String[] toks = version.split("\\.");
            StringBuilder sb = new StringBuilder();
            sb.append(toks[0]).append('.').append(toks[1]).append('.')
                    .append(toks[2]);
            toks = toks[3].split("-");
            if(toks.length > 1) {
                sb.append('-').append(Strings.join("-",
                        (Object[]) Arrays.copyOfRange(toks, 1, toks.length)));
            }
            sb.append('+').append(toks[0]);
            version = sb.toString();
        }
        return Version.valueOf(version);
    }

    private Versions() {/* no-op */}

}
