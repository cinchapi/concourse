/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

/**
 * An object that can be used to programmatically interact with a local instance
 * of the Concourse codebase.
 * 
 * @author Jeff Nelson
 * @deprecated use
 *             {@link com.cinchapi.concourse.automation.developer.ConcourseCodebase}
 *             instead
 */
@Deprecated
public class ConcourseCodebase {

    /**
     * If necessary, clone the codebase from Github to a local directory and
     * return a handler than can be used for programmatic interaction. Multiple
     * attempts to clone the codebase from Github will return the same
     * {@link ConcourseCodebase codebase object} for the lifetime of the JVM
     * process.
     * 
     * @return the handler to interact with the codebase
     */
    public static ConcourseCodebase cloneFromGithub() {
        return new ConcourseCodebase();
    }

    /**
     * The canonical codebase where this class forwards functionality.
     */
    private final com.cinchapi.concourse.automation.developer.ConcourseCodebase codebase;

    /**
     * Construct a new instance.
     */
    private ConcourseCodebase() {
        this.codebase = com.cinchapi.concourse.automation.developer.ConcourseCodebase
                .get();
    }

    /**
     * Create a concourse-server.bin installer from this
     * {@link ConcourseCodebase codebase} and return the path to the installer
     * file.
     * 
     * @return the path to the installer file
     */
    public String buildInstaller() {
        return codebase.installer().toString();
    }

    /**
     * Return the path to this {@link ConcourseCodebase codebase}.
     * 
     * @return the path to the codebase
     */
    public String getPath() {
        return codebase.path().toString();
    }

}
