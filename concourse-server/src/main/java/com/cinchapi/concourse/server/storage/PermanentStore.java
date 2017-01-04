/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
 * A {@link Store} that accepts {@link Write} objects that are transported from
 * a {@link Limbo}. This service relies on rich indexing to offer optimal
 * read performance.
 * 
 * @author Jeff Nelson
 */
public interface PermanentStore extends Store {

    /**
     * Process and store {@code write}.
     * 
     * @param write
     */
    public void accept(Write write);

    /**
     * Process and store {@code write} while obeying the directive to
     * {@code sync} or not. A sync guarantees that the write is durably
     * persisted.
     * 
     * @param write
     * @param sync
     */
    public void accept(Write write, boolean sync);

    /**
     * Force the store to sync all of its writes to disk to guarantee that they
     * are durably persisted. Generally, this method will "fsync" pending writes
     * that {@link #accept(Write, boolean) were not synced when accepted}. For
     * example, this is a way to enable <em>group sync</em> functionality.
     */
    public void sync();
}
