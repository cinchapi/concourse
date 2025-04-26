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
package com.cinchapi.concourse.server.storage.transporter;

import com.cinchapi.concourse.server.storage.Store;

/**
 * A {@link Store} that can provide {@link Batch batches} of
 * {@link Write writes} for indexing by a {@link BatchTransporter}.
 *
 * @author Jeff Nelson
 */
public interface BatchTransportable extends Store {

    /**
     * Retrieve the next {@link Batch batch} that is ready for transport,
     * waiting if necessary until one becomes available.
     * <p>
     * This method blocks the current thread until a {@link Batch batch} ready
     * for processing. The implementation determines the batching strategy,
     * including size and timing considerations.
     * </p>
     * 
     * @return a {@link Batch} containing writes to be indexed
     * @throws InterruptedException if the thread is interrupted while waiting
     *             for a batch to become available
     */
    Batch nextBatch() throws InterruptedException;

    /**
     * Purge a {@link Batch batch} that has been successfully transported and is
     * therefore accessible in a different {@link Store} and no longer part of
     * this {@link Store store's} state.
     * 
     * @param batch the {@link Batch} that has been successfully processed
     */
    void purge(Batch batch);
}
