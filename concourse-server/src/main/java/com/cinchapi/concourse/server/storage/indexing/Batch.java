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
package com.cinchapi.concourse.server.storage.indexing;

import com.cinchapi.concourse.server.storage.temp.Write;

/**
 * A container for an array of {@link Write Writes} that should be processed
 * together by a {@link BatchTransporter} during a single
 * {@link BatchTransporter#process() pass}.
 * <p>
 * Each {@link Batch} has an ordinal position in the sequence of Batches that
 * have been queued for processing, which facilitates asynchronous indexing
 * while ensuring that any generated {@link Segment segments} are appended in
 * order.
 * <p>
 * <strong>NOTE:</strong>The {@link Write Writes} in a Batch are stored in order
 * of application from oldest to newest.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Batch {

    /**
     * The array of {@link Write} operations contained in this Batch.
     */
    private final Write[] writes;

    /**
     * The ordinal position of this Batch in the sequence of Batches
     * that have been queued for processing.
     */
    private final int ordinal;

    /**
     * Construct a new Batch.
     * 
     * @param writes the array of {@link Write} operations in order from oldest
     *            to newest
     * @param ordinal the position of this Batch in the processing sequence
     */
    public Batch(Write[] writes, int ordinal) {
        this.writes = writes;
        this.ordinal = ordinal;
    }

    /**
     * Return the number of {@link Write} operations in this Batch.
     * 
     * @return the size of the Batch
     */
    public int size() {
        return writes.length;
    }

    /**
     * Return the array of {@link Write} operations contained in this Batch.
     * The writes are ordered from oldest to newest.
     * 
     * @return the array of {@link Write} operations
     */
    public Write[] writes() {
        return writes;
    }

    /**
     * Return the ordinal position of this Batch in the sequence of
     * Batches that have been queued for processing.
     * 
     * @return the ordinal position
     */
    public int ordinal() {
        return ordinal;
    }
}
