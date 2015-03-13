/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.util;

import static org.cinchapi.concourse.server.GlobalState.DEFAULT_ENVIRONMENT;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cinchapi.concourse.server.GlobalState;

import com.google.common.base.Strings;

/**
 * Utility methods related to Environments.
 * 
 * @author Jeff Nelson
 */
public final class Environments {

    /**
     * The pattern that is used for sanitizing the environment. We compile it
     * statically so that we can avoid the overhead of doing it for every
     * sanitization request.
     */
    private static final Pattern SANITIZER = Pattern.compile("[^A-Za-z0-9_]");

    /**
     * Ensure that we return the correct environment with
     * alphanumeric-char name for the specified {@code env}.
     * e.g. if {@code env} is null or empty then return the
     * {@link GlobalState#DEFAULT_ENVIRONMENT}.
     * 
     * @param env
     * @return the environment name
     */
    public static String sanitize(String env) {
        env = Strings.isNullOrEmpty(env) ? DEFAULT_ENVIRONMENT : env;
        Matcher matcher = SANITIZER.matcher(env);
        env = matcher.replaceAll(""); // ConcourseServer checks to make sure
                                      // sanitizing the default environment
                                      // won't turn it into an empty string
        return env;
    }

    private Environments() {/* noop */}

}
