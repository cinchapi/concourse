/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import java.nio.file.Path;

/**
 * A {@link Freezable} object is initially mutable and maintained entirely
 * in-memory upon creation. After it is {@link #freeze(Path, long) frozen}
 * within a {@link Path file} it is no longer mutable and its data may be
 * streamed into memory on-demand.
 *
 * @author Jeff Nelson
 */
public interface Freezable {

    /**
     * Freeze this object, mark its data as safe to remove from memory and
     * stream the same data from {@code file} on-demand.
     * 
     * @param file
     */
    public default void freeze(Path file) {
        freeze(file, 0);
    }

    /**
     * Freeze this object, mark its data as safe to remove from memory and
     * stream the same data from {@code file}, (starting at {@code position})
     * on-demand.
     * 
     * @param file
     * @param position
     */
    public void freeze(Path file, long position);

    /**
     * Return {@code true} if this object has been {@link #freeze(Path, long)
     * frozen}.
     * 
     * @return a boolean that indicates if the object is frozen
     */
    public boolean isFrozen();

}
