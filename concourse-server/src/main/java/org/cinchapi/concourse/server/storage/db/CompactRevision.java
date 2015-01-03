/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.db;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.Versioned;

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
 * @author jnelson
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
