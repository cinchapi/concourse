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
package com.cinchapi.concourse.server.storage;

import java.util.stream.Stream;

import com.cinchapi.concourse.server.storage.temp.Write;

/**
 * Able to return a {@link Stream} of {@link Write Writes}.
 * <p>
 * A {@link WriteStream} is backed by a stable collection of {@link Write
 * Writes} that may allow {@link #append(Write) appending}, but guarantees that
 * the underlying collection will not be modified while {@link #writes()
 * streaming}.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface WriteStream {

    /**
     * Append {@code write} to the {@link Stream}.
     * 
     * @param write
     */
    public default void append(Write write) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return a {@link Stream} of {@link Write writes}.
     */
    public Stream<Write> writes();

}
