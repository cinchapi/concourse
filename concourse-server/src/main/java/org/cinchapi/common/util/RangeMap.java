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
package org.cinchapi.common.util;

import java.util.Set;

/**
 * A {@link RangeMap} simulates the mapping from disjoint intervals of points to
 * values. Conceptually, you can think of an entry in a RangeMap as representing
 * something like <em>all the points between X and Y mapping to V</em>.
 * <p>
 * It is important to understand the relationships among ranges, points and
 * values. Ranges are made up of points. When adding to the map, you explicitly
 * associate a range to a value and all the points within that range are
 * implicitly covered by the value. And since ranges can <em>range</em> (no pun
 * intended) from a single point to broad (possibly infinite) intervals and
 * overlap, it is possible for a single point to be covered by multiple values.
 * </p>
 * <p>
 * A RangeMap allows you to uncover relationships between points and values,
 * without explicitly declaring them. For example, you can map range
 * {@code (0,4)} to {@code X} and range {@code [2, 7)} to {@code Y} and ask
 * which values cover {@code 3}. The RangeMap will return a set containing
 * values X and Y even though you never added an explicit mapping from {@code 3}
 * to {@code X} or {@code Y}.
 * </p>
 * 
 * @author jnelson
 */
public interface RangeMap<P extends Comparable<P>, V> {

    /**
     * Return {@code true} if the map contains at least one value associated
     * with the {@code point}. Values can be associated with a point if a
     * mapping from that point was added directly, or if that point is contained
     * within another range that has associated values within the map.
     * 
     * @param point
     * @return {@code true} if the point is contained
     */
    public boolean contains(P point);

    /**
     * Return {@code true} if the map contains any values associated with
     * <strong>at least one</strong> of the points in {@code range}. Values can
     * be associated with a range if a mapping from that range was added
     * directly or if that range is contained within another range that has
     * associated values within the map.
     * 
     * @param range
     * @return {@code true} if the range is contained
     */
    public boolean contains(Range<P> range);

    /**
     * Put a mapping from all the points in the {@code range} to {@code value}.
     * 
     * @param range
     * @param value
     * @return {@code true} if there is an increase in coverage (e.g. at least
     *         one point in the range maps to {@code value} now whereas it did
     *         not before.
     */
    public boolean put(Range<P> range, V value);

    /**
     * Remove any existing mappings from all the points in the {@code range} to
     * {@code value}.
     * 
     * @param range
     * @param value
     * @return {@code true} if there is a decrease in coverage (e.g. at least
     *         one point in the range mapped to {@code value} previously, but no
     *         longer does
     */
    public boolean remove(Range<P> range, V value);

    /**
     * Return all the values that are mapped from {@code point}. Values can be
     * associated with a point if a mapping from that point was added directly,
     * or if that point is contained within another range that has associated
     * values within the map.
     * 
     * @param point
     * @return a collection of all the values that are mapped from ranges
     *         containing {@code point}
     */
    public Set<V> get(P point);

    /**
     * Return all the values that are mapped from {@code range}. Values can be
     * associated with a range if a mapping from that range was added directly
     * or if that range is contained within another range that has associated
     * values within the map.
     * 
     * @param range
     * @return a collection of all the values that are mapped from ranges
     *         <em>intersecting</em> {@code range}
     */
    public Set<V> get(Range<P> range);

}
