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

/**
 * A mock implementation of {@link ConcourseRuntime} to use in unit tests.
 * 
 * @author Jeff Nelson
 */
public class MockConcourseRuntime {

    /**
     * Return the environment of the executing
     * {@link ConcourseRuntimeAuthorized} thread.
     * 
     * @return the thread environment
     */
    public String environment() {
        try {
            ConcourseRuntimeAuthorized thread = (ConcourseRuntimeAuthorized) Thread
                    .currentThread();
            return thread.environment();
        }
        catch (ClassCastException e) {
            throw new RuntimeException("Illegal attempt to use "
                    + MockConcourseRuntime.class.getSimpleName()
                    + " from an unsupported thread");
        }
    }

}
