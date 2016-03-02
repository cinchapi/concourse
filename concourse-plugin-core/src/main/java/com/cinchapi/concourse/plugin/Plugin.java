/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin;

/**
 * This is the abstract class for all {@link Plugin plugins} that extend the
 * capabilities of Concourse Server with additional functionality that is
 * accessible from a client driver.
 * 
 * @author Jeff Nelson
 */
public abstract class Plugin {

    /**
     * A reference to the {@link ConcourseRuntime backend} where this plugin is
     * registered.
     */
    protected final ConcourseRuntime concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse the {@link ConcourseRuntime} where this plugin is
     *            registered
     */
    protected Plugin(ConcourseRuntime concourse) {
        this.concourse = concourse;
    }

}
