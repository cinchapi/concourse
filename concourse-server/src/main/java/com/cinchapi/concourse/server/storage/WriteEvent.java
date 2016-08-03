/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.server.storage.temp.Write;

/**
 * Wrapper class that holds reference to {@link Write} and {@code environment}
 * that is associated with {@link Engine}
 */
public class WriteEvent {

    /**
     * {@link Write} to store
     */
    private Write write;

    /**
     * environment associated with {@link Engine }
     */
    private String environment;

    /**
     * Construct a new instance that is made up of {@link Write} and {@code
     * environment}
     *
     * @param write reference to the {@link Write} instance
     * @param environment environment associated with {@link Engine }
     */
    public WriteEvent(final Write write, final String environment) {
        this.write = write;
        this.environment = environment;
    }

    /**
     * Return {@link} Write
     */
    public Write getWrite() {
        return write;
    }

    /**
     * Return {@code environment} associated with {@link Engine}
     */
    public String getEnvironment() {
        return environment;
    }
}
