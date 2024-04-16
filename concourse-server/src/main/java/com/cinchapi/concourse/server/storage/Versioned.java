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
package com.cinchapi.concourse.server.storage;

/**
 * A storable object that is versioned by a {@link Long} (e.g. a timestamp).
 * <p>
 * The version is stored directly with the object so that it does not change
 * when the object's storage context changes (e.g. transport from a
 * {@link com.cinchapi.concourse.server.storage.temp.Limbo} to
 * {@link com.cinchapi.concourse.server.storage.DurableStore DurableStore}, data
 * replication, cluster rebalance, etc).
 * </p>
 * <p>
 * Versions are unique <em>across commits</em>, but objects within the same
 * commit (e.g.
 * an {@link com.cinchapi.concourse.server.storage.AtomicOperation atomic
 * operation}) may have the same version. In that case, the object's can be
 * distinguished and sequence by their {@link #stamp() stamp}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public interface Versioned {

    /**
     * Represents a {@code null} version, which indicates that the object is
     * not {@link #isStorable() storable}.
     */
    public static final long NO_VERSION = 0;

    /**
     * Return {@code true} if {@code obj} is <em>logically</em> equal to this
     * one, meaning all of its attributes other than its {@code version} are
     * equal to those in this object.
     */
    @Override
    public boolean equals(Object obj);

    /**
     * Return this object's version.
     * <p>
     * For non-{@link #isStorable() storable} objects, the value returned is
     * always {@link #NO_VERSION}.
     * </p>
     * 
     * @return the version
     */
    public long getVersion();

    /**
     * Return the <em>logical</em> hash code value for this object, which does
     * not take the {@code version} into account.
     * 
     * @return the hash code
     */
    @Override
    public int hashCode();

    /**
     * Return {@code true} if the object's version is not equal to
     * {@link #NO_VERSION}.
     * 
     * @return {@code true} if the object is storable
     */
    public default boolean isStorable() {
        return getVersion() != NO_VERSION;
    }

    /**
     * Return this object's {@link #stamp() stamp}.
     * <p>
     * The {@link #stamp() stamp} is a globally unique value that distinguishes
     * objects regardless of their {@link #getVersion() versions}.
     * </p>
     * <p>
     * The primary purpose of a {@link #stamp() stamp} is to provide relative
     * 1) disambiguation and 2) sequencing (e.g. determining the order of
     * operations in a {@link com.cinchapi.concourse.server.storage.Transaction
     * transaction}) Unlike {@link #getVersion() versions}, {@link #stamp()
     * Stamps} are transient and are subject to change whenever an object is
     * created or read from storage.
     * </p>
     * <p>
     * Given these constraints, {@link #stamp() stamps} should always be
     * assigned in monotonically increasing order.
     * </p>
     * 
     * @return the stamp
     */
    public long stamp();

}
