/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

/**
 * An object that is versioned by a unique 8 byte long (i.e. a timestamp). The
 * version is stored directly with the object so that it does not change when
 * the object's storage context changes (i.e. buffer transport, data
 * replication, cluster rebalance, etc).
 * 
 * @author jnelson
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
