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

/**
 * An object that is versioned by a unique 8 byte long (i.e. a timestamp). The
 * version is stored directly with the object so that it does not change when
 * the object's storage context changes (i.e. buffer transport, data
 * replication, cluster rebalance, etc).
 * 
 * @author Jeff Nelson
 */
public interface Versioned {

    /**
     * Represents a {@code null} version, which indicates that the object is
     * notStorable.
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
     * Return this object's version, which is unique amongst storable objects.
     * For notStorable objects, the version is always equal to
     * {@link #NO_VERSION}.
     * 
     * @return the {@code version}
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
    public boolean isStorable();

}
