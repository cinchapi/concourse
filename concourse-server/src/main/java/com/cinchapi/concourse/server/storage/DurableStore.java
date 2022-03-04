/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.util.Set;

import com.cinchapi.concourse.server.storage.temp.Write;
import com.google.common.hash.HashCode;

/**
 * A {@link Store} that {@link #accept(Write) accepts} {@link Write Writes} and
 * can guarantee that they are {@link #sync() durably persisted} individually or
 * as a group.
 * 
 * @author Jeff Nelson
 */
public interface DurableStore extends Store {

    /**
     * Insert {@code write} into this {@link Store}.
     * <p>
     * The default durability guarantee provided this method varies among
     * {@link DurableStore stores}. If explicit control over durability is
     * required, use the {@link #accept(Write, boolean)} method and set the
     * {@code sync} parameter accordingly
     * </p>
     * 
     * @param write
     */
    public void accept(Write write);

    /**
     * Insert {@code write} into this {@link Store} while obeying the directive
     * to {@code sync} or not. A {@link #sync()} guarantees that the
     * {@code write} is durably persisted.
     * 
     * @param write
     * @param sync a boolean that determines if the {@link Store} should perform
     *            a {@link #sync()} after accepting the {@code write}
     */
    public void accept(Write write, boolean sync);

    /**
     * If necessary, reconcile the state of this {@link Store} and prepare it to
     * {@link #accept(Write) accept} all the {@link Write writes} represented by
     * each of the {@code hashes}.
     * <p>
     * This exact behaviour of this method is undefined, but the implementing
     * class is expected to prepare itself in such a way that inserting
     * a {@link Write Writes} with each of the {@code hashes} does not alter
     * overall data consistency or durability.
     * </p>
     * 
     * @param hashes
     */
    public default void reconcile(Set<HashCode> hashes) {/* no-op */}

    /**
     * Force this {@link Store} to guarantee that all the {@link Write Writes}
     * that have been {@link #accept(Write) accepted} are durably persisted.
     * <p>
     * It is possible to force the {@link Store} to guarantee the durability of
     * each {@link #accept(Write, boolean) accepted} {@link Write} individually
     * by setting the {@code sync} parameter to {@code true} when calling the
     * {@link #accept(Write, boolean)} method.
     * <p>
     * However, there are cases when it is preferable to enable <em>group
     * sync</em> functionality where several {@link Write Writes} are
     * {@link #accept(Write) accepted} without being immediately synced
     * (<em>e.g. a {@link Transaction} that guarantees durability by taking a
     * backup prior to {@link Transaction#commit() committing}</em>). In such
     * cases, this {@link #sync()} method allows for the guarantee that those
     * {@link Writes Writes} will all be durably persisted in the same way they
     * would have been if they were immediately sycned when
     * {@link #accept(Write) accepted}.
     * </p>
     * <p>
     * Implementation of this functionality usually involves performing an
     * "fsync" when transferring the {@link Write Writes} to disk.
     * </p>
     */
    public void sync();
}
