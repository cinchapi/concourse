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
package com.cinchapi.concourse.server.model;

import java.util.Comparator;
import java.util.List;

import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.BoundType;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 * A collection of utilities to augment the {@link Range} class with respect to
 * coverage of {@link Value Values}.
 * 
 * @author Jeff Nelson
 */
public final class Ranges {

    /**
     * Compare {@code a} and {@code b} by their lower endpoints and, if
     * necessary, bounds.
     * 
     * @param a
     * @param b
     * @return a negative Value, zero, or a positive Value as {@code a} is
     *         less than, equal to, or greater than {@code b}.
     */
    public static int compareToLower(Range<Value> a, Range<Value> b) {
        return ComparisonChain
                .start()
                .compare(getLowerEndpoint(a), getLowerEndpoint(b))
                .compare(getLowerBoundType(a), getLowerBoundType(b),
                        LOWER_BOUND_COMPARATOR).result();
    }

    /**
     * Compare {@code a} and {@code b} by their upper endpoints and, if
     * necessary, bounds.
     * 
     * @param a
     * @param b
     * @return a negative Value, zero, or a positive Value as {@code a} is
     *         less than, equal to, or greater than {@code b}.
     */
    public static int compareToUpper(Range<Value> a, Range<Value> b) {
        return ComparisonChain
                .start()
                .compare(getUpperEndpoint(a), getUpperEndpoint(b))
                .compare(getUpperBoundType(a), getUpperBoundType(b),
                        UPPER_BOUND_COMPARATOR).result();
    }

    /**
     * Convert the given {@code range} for {@code key} to a matching
     * {@link RangeToken}.
     * 
     * @param key
     * @param range
     * @return the RangeToken
     */
    public static RangeToken convertToRangeToken(Text key, Range<Value> range) {
        Value lower = getLowerEndpoint(range);
        Value upper = getUpperEndpoint(range);
        boolean lowerClosed = getLowerBoundType(range) == BoundType.CLOSED;
        boolean upperClosed = getUpperBoundType(range) == BoundType.CLOSED;
        // We use the length of the values array in the RangeToken as a hack to
        // signal what kind of non-traditional (i.e. closedOpen) bounds to use
        // on the BETWEEN range when using the Guava data type.
        int size;
        if(!lowerClosed && !upperClosed) {
            size = 3;
        }
        else if(lowerClosed && upperClosed) {
            size = 4;
        }
        else if(!lowerClosed && upperClosed) {
            size = 5;
        }
        else {
            size = 2;
        }
        Value[] values = new Value[size];
        values[0] = lower;
        values[1] = upper;
        for (int i = 2; i < values.length; i++) {
            values[i] = Value.NEGATIVE_INFINITY;
        }
        return RangeToken.forReading(key, Operator.BETWEEN, values);
    }

    /**
     * Equivalent to {@link Range#lowerBoundType()} except that
     * {@link BoundType#CLOSED} is returned if the lower endpoint is equals to
     * {@link Value#NEGATIVE_INFINITY}.
     * 
     * @param range
     * @return the lower bound type
     */
    public static BoundType getLowerBoundType(Range<Value> range) {
        Value lower = getLowerEndpoint(range);
        if(lower == Value.NEGATIVE_INFINITY) {
            return BoundType.CLOSED;
        }
        else {
            return range.lowerBoundType();
        }
    }

    /**
     * Equivalent to {@link Range#lowerEndpoint()} except that
     * {@link Value#NEGATIVE_INFINITY} is returned if the {@code range} does not
     * have a defined lower bound.
     * 
     * @param range
     * @return the lower endpoint
     */
    public static Value getLowerEndpoint(Range<Value> range) {
        if(!range.hasLowerBound()) {
            return Value.NEGATIVE_INFINITY;
        }
        else {
            return range.lowerEndpoint();
        }
    }

    /**
     * Equivalent to {@link Range#upperBoundType()} except that
     * {@link BoundType#CLOSED} is returned if the upper endpoint is equals to
     * {@link Value#POSITVE_INFINITY}.
     * 
     * @param range
     * @return the upper bound type
     */
    public static BoundType getUpperBoundType(Range<Value> range) {
        Value upper = getUpperEndpoint(range);
        if(upper == Value.POSITIVE_INFINITY) {
            return BoundType.CLOSED;
        }
        else {
            return range.upperBoundType();
        }
    }

    /**
     * Equivalent to {@link Range#upperEndpoint()} except that
     * {@link Value#POSITVE_INFINITY} is returned if the {@code range} does not
     * have a defined upper bound.
     * 
     * @param range
     * @return the upper endpoint
     */
    public static Value getUpperEndpoint(Range<Value> range) {
        if(!range.hasUpperBound()) {
            return Value.POSITIVE_INFINITY;
        }
        else {
            return range.upperEndpoint();
        }
    }

    /**
     * Return a new {@link Range} that is the merger (e.g. union) of {@code a}
     * and {@code b}. The new {@link Range} maintains both the lower and higher
     * endpoint/bound between the two inputs.
     * 
     * @param a
     * @param b
     * @return the union of {@code a} and {@code b}
     */
    public static Range<Value> merge(Range<Value> a, Range<Value> b) {
        if(a.isConnected(b)) {
            boolean aStart = compareToLower(a, b) < 0;
            boolean aEnd = compareToUpper(a, b) > 0;
            boolean lower = getLowerBoundType(aStart ? a : b) == BoundType.CLOSED;
            boolean upper = getUpperBoundType(aStart ? a : b) == BoundType.CLOSED;
            if(lower && upper) {
                return Range.closed(getLowerEndpoint(aStart ? a : b),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else if(!lower && upper) {
                return Range.closedOpen(getLowerEndpoint(aStart ? a : b),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else if(lower && !upper) {
                return Range.openClosed(getLowerEndpoint(aStart ? a : b),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else {
                return Range.open(getLowerEndpoint(aStart ? a : b),
                        getUpperEndpoint(aEnd ? a : b));
            }
        }
        else {
            return null;
        }
    }

    /**
     * Return the ranges that include the points that are in {@code a} or the
     * {@code b} one and not in their intersection. The return set will include
     * between 0 and 2 ranges that together include all the points that meet
     * this criteria.
     * <p>
     * <strong>NOTE:</strong> If the two ranges do not intersect, then a
     * collection containing both of them is returned (since they already form
     * their xor).
     * </p>
     * 
     * @param a
     * @param b
     * @return the set or ranges that make uValue the symmetric difference
     *         between this range and the {@code other} one
     */
    public static Iterable<Range<Value>> xor(Range<Value> a, Range<Value> b) {
        List<Range<Value>> ranges = Lists.newArrayList();
        try {
            Range<Value> intersection = a.intersection(b);
            boolean aStart = compareToLower(a, b) < 0;
            boolean aEnd = compareToUpper(a, b) > 0;
            boolean lower = getLowerBoundType(aStart ? a : b) == BoundType.CLOSED;
            boolean upper = getUpperBoundType(aEnd ? a : b) == BoundType.CLOSED;
            boolean interLower = getLowerBoundType(intersection) == BoundType.OPEN;
            boolean interUpper = getUpperBoundType(intersection) == BoundType.OPEN;
            Range<Value> first;
            if(lower && interLower) {
                first = Range.closed(getLowerEndpoint(aStart ? a : b),
                        getLowerEndpoint(intersection));
            }
            else if(!lower && interLower) {
                first = Range.openClosed(getLowerEndpoint(aStart ? a : b),
                        getLowerEndpoint(intersection));
            }
            else if(lower && !interLower) {
                first = Range.closedOpen(getLowerEndpoint(aStart ? a : b),
                        getLowerEndpoint(intersection));
            }
            else {
                first = Range.open(getLowerEndpoint(aStart ? a : b),
                        getLowerEndpoint(intersection));
            }
            Range<Value> second;
            if(interUpper && upper) {
                second = Range.closed(getUpperEndpoint(intersection),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else if(!interUpper && upper) {
                second = Range.openClosed(getUpperEndpoint(intersection),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else if(interUpper && !interUpper) {
                second = Range.closedOpen(getUpperEndpoint(intersection),
                        getUpperEndpoint(aEnd ? a : b));
            }
            else {
                second = Range.open(getUpperEndpoint(intersection),
                        getUpperEndpoint(aEnd ? a : b));
            }
            if(!first.isEmpty()) {
                ranges.add(first);
            }
            if(!second.isEmpty()) {
                ranges.add(second);
            }
        }
        catch (IllegalArgumentException e) { // ranges dont intersect
            ranges.add(a);
            ranges.add(b);
        }
        return ranges;
    }

    /**
     * A comparator to sort the lower bound of Ranges.
     */
    private final static Comparator<BoundType> LOWER_BOUND_COMPARATOR = new Comparator<BoundType>() {

        @Override
        public int compare(BoundType o1, BoundType o2) {
            if(o1 == o2) {
                return 0;
            }
            else if(o1 == BoundType.CLOSED) {
                return -1;
            }
            else {
                return 1;
            }
        }

    };

    /**
     * A comparator to sort the upper bound of Ranges.
     */
    private final static Comparator<BoundType> UPPER_BOUND_COMPARATOR = new Comparator<BoundType>() {

        @Override
        public int compare(BoundType o1, BoundType o2) {
            if(o1 == o2) {
                return 0;
            }
            else if(o1 == BoundType.CLOSED) {
                return 1;
            }
            else {
                return -1;
            }
        }

    };

    private Ranges() {/* noop */}

}
