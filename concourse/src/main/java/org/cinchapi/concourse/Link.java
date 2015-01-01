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
package org.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

/**
 * A {@link Link} is a wrapper around a {@link Long} that represents the primary
 * key of a record and distinguishes from simple long values. A Link is
 * returned from the {@code #fetch} or {@code #get} methods in {@link Concourse}
 * if data was added using one of the {@code #link} operations.
 * 
 * @author jnelson
 */
// NOTE: This class extends Number so that it can be treated like other
// numerical Values during comparisons when the data is stored and sorted in a
// SecondaryIndex for efficient range queries.
@Immutable
public final class Link extends Number implements Comparable<Link> {

    /**
     * Return a Link that points to {@code record}.
     * 
     * @param record
     * @return the Link
     */
    public static Link to(long record) {
        return new Link(record);
    }

    private static final long serialVersionUID = 1L; // Serializability is
                                                     // inherited from {@link
                                                     // Number}.
    /**
     * The signed representation for the primary key of the record to which this
     * Link points.
     */
    private final long record;

    /**
     * Construct a new instance.
     * 
     * @param record
     */
    private Link(long record) {
        this.record = record;
    }

    @Override
    public int compareTo(Link other) {
        return UnsignedLongs.compare(longValue(), other.longValue());
    }

    @Override
    public double doubleValue() {
        return (double) record;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Link) {
            Link other = (Link) obj;
            return UnsignedLongs.compare(record, other.record) == 0;
        }
        return false;
    }

    @Override
    public float floatValue() {
        return (float) record;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(record);
    }

    @Override
    public int intValue() {
        return (int) record;
    }

    @Override
    public long longValue() {
        return record;
    }

    @Override
    public String toString() {
        return UnsignedLongs.toString(longValue()); // for
                                                    // compatibility
                                                    // with
                                                    // {@link
                                                    // com.cinchapi.common.Numbers.compare(Number,
                                                    // Number)}
    }
}
