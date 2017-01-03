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
package com.cinchapi.concourse.util;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A utility class to compliment the methods that are contained in th built-in
 * {@link Paths} utility class.
 * 
 * @author Jeff Nelson
 */
public final class MorePaths {

    /**
     * Implementation of the {@link Paths#get(String, String...)} method that
     * handles weird platform-dependent behaviour.
     * 
     * @param first the path string or initial part of the path string
     * @param more additional parts that should be joined to form the path
     *            string
     * @return the {@link Path} based on the supplied components
     */
    public static Path get(String first, String... more) {
        for (int i = 0; i < more.length; ++i) {
            more[i] = sanitize(more[i]);
        }
        return Paths.get(sanitize(first), more);
    }

    /**
     * Sanitize the given URI with respect to the underlying platform by
     * removing any character that don't belong.
     * 
     * @param uri the uri to sanitize
     * @return the sanitized version of the URI
     */
    private static String sanitize(String uri) {
        if(Platform.isWindows()
                && (uri.startsWith("/") || uri.startsWith("\\"))) {
            uri = uri.substring(1);
        }
        return uri;
    }

    private MorePaths() {/* no-op */}

}
