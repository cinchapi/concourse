/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import static org.cinchapi.concourse.server.GlobalState.DEFAULT_ENVIRONMENT;

import org.cinchapi.concourse.server.GlobalState;

import com.google.common.base.Strings;

/**
 * Utility methods related to Environments.
 * 
 * @author jnelson
 */
public final class Environments {

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
        env = env.replaceAll("[^A-Za-z0-9]", ""); // ConcourseServer checks to
                                                  // make sure sanitizing the
                                                  // default environment won't
                                                  // turn it into the empty
                                                  // string
        return env;
    }

    private Environments() {/* noop */}

}
