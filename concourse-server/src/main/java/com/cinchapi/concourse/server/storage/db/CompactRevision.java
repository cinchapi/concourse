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
package com.cinchapi.concourse.server.storage.db;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.Versioned;

/**
 * A compact form of a {@link Revision} that is appropriate to store within a
 * {@link Record records} collection of history in order to save space.
 * <p>
 * Compared to a normal Revision, this is more memory efficient because it only
 * contains the value, version and type involved with the revision. This is okay
 * to do because it is assumed that the housing Record has information about the
 * locator and key.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
class CompactRevision<V extends Comparable<V>> implements Versioned {

    /**
     * A field indicating the action performed to generate this Revision. This
     * information is recorded so that we can efficiently purge history while
     * maintaining consistent state.
     */
    private final Action type;

    /**
     * The tertiary component that typically represents the payload for what
     * this Revision represents.
     */
    private final V value;

    /**
     * The unique version that identifies this Revision. Versions are assumed to
     * be an atomically increasing values (i.e. timestamps).
     */
    private final long version;

    /**
     * Construct a new instance.
     * 
     * @param value
     * @param timestamp
     * @param type
     */
    CompactRevision(V value, long version, Action type) { /* package-private */
        this.value = value;
        this.version = version;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof CompactRevision) {
            return value.equals(((CompactRevision<V>) obj).value);
        }
        else {
            return false;
        }
    }

    /**
     * Return the {@link #type} associated with this Revision.
     * 
     * @return the type
     */
    public Action getType() {
        return type;
    }

    /**
     * Return the {@link #value} associated with this Revision.
     * 
     * @return the value
     */
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean isStorable() {
        return false;
    }

    @Override
    public String toString() {
        return type + " " + value + " AT " + version;
    }

    /**
     * Return a string description that is compatible with a full
     * {@link Revision} based on the {@code locator} and {@code key}.
     * 
     * @return toString output
     */
    public <L extends Comparable<L>, K extends Comparable<K>> String toString(
            L locator, K key) {
        return type + " " + key + " AS " + value + " IN " + locator + " AT "
                + version;
    }

}
