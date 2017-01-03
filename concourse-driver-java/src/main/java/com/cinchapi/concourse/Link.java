/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

/**
 * A {@link Link} is a pointer to a record.
 * <p>
 * Links should never be written directly. They can be created using the
 * {@link Concourse#link(String, long, long) link()} methods in the
 * {@link Concourse} API.
 * </p>
 * <p>
 * Links may be returned when reading data using the
 * {@link Concourse#select(long)
 * select()}, {@link Concourse#get(Object) get()} and
 * {@link Concourse#browse(String) browse()} methods. When handling Link
 * objects, you can retrieve the underlying record id by calling
 * {@link Link#longValue()}.
 * </p>
 * <p>
 * When performing a
 * {@link Concourse#insert(com.google.common.collect.Multimap) bulk insert}, you
 * can use this class to create Link objects that are added to the data/json
 * blob. Links inserted in this manner will be written in the same way they
 * would have been if they were written using the
 * {@link Concourse#link(String, long, java.util.Collection) link()} API
 * methods.
 * </p>
 * <p>
 * To create a static link to a single record, use {@link Link#to(long)}.
 * </p>
 * <p>
 * To create static links to each of the records that match a criteria, use one
 * of the {@link Link#toWhere(Criteria) Link.toWhere()} methods (see the
 * documentation for {@link com.cinchapi.concourse.util.Convert.ResolvableLink
 * resolvable links} for more information).
 * </p>
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
     * @param record the record id
     * @return a {@link Link} that points to {@code record}
     */
    public static Link to(long record) {
        return new Link(record);
    }

    /**
     * Return a string that instructs Concourse to create links that point to
     * each of the records that match the {@code ccl} string.
     * 
     * <p>
     * <strong>NOTE:</strong> This method DOES NOT return a {@link Link} object,
     * so it should only be used when adding a
     * {@link com.cinchapi.concourse.util.Convert.ResolvableLink resolvable
     * link} value to a data/json blob that will be passed to the
     * {@link Concourse#insert(com.google.common.collect.Multimap)
     * insert()} methods.
     * </p>
     * 
     * @param ccl a CCL string that describes the records to which a Link should
     *            point
     * @return a {@link com.cinchapi.concourse.util.Convert.ResolvableLink
     *         resolvable link instruction}
     */
    public static String toWhere(String ccl) {
        return Convert.stringToResolvableLinkInstruction(ccl);
    }

    /**
     * Return a string that instructs Concourse to create links that point to
     * each of the records that match the {@code ccl} string.
     * 
     * <p>
     * <strong>NOTE:</strong> This method DOES NOT return a {@link Link} object,
     * so it should only be used when adding a
     * {@link com.cinchapi.concourse.util.Convert.ResolvableLink resolvable
     * link} value to a data/json blob that will be passed to the
     * {@link Concourse#insert(com.google.common.collect.Multimap)
     * insert()} methods.
     * </p>
     * 
     * @param criteria a {@link Criteria} that describes the records to which a
     *            Link should point
     * @return a {@link com.cinchapi.concourse.util.Convert.ResolvableLink
     *         resolvable link instruction}
     */
    public static String toWhere(Criteria criteria) {
        return toWhere(criteria.getCclString());
    }

    /**
     * Return a string that instructs Concourse to create links that point to
     * each of the records that match the {@code ccl} string.
     * 
     * <p>
     * <strong>NOTE:</strong> This method DOES NOT return a {@link Link} object,
     * so it should only be used when adding a
     * {@link com.cinchapi.concourse.util.Convert.ResolvableLink resolvable
     * link} value to a data/json blob that will be passed to the
     * {@link Concourse#insert(com.google.common.collect.Multimap)
     * insert()} methods.
     * </p>
     * 
     * @param criteria a criteria builder in a {@link BuildableState} that
     *            describes the records to which a Link should point
     * @return a {@link com.cinchapi.concourse.util.Convert.ResolvableLink
     *         resolvable link instruction}
     */
    public static String toWhere(Object criteria) {
        Preconditions.checkArgument(criteria instanceof BuildableState,
                Strings.format("{} is not a valid criteria", criteria));
        return toWhere(((BuildableState) criteria).build());
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
        return "@" + UnsignedLongs.toString(longValue());
    }
}
