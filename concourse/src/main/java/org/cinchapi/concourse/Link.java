/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
 * @author Jeff Nelson
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
