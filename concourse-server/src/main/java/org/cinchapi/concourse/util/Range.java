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
package org.cinchapi.concourse.util;

// TODO move to org.cinchapi.concourse.server.model

import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.model.Value;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Sets;

/**
 * A {@link Range} is an interval between two points that refers to
 * all the values that lie in between. <h2>Range Construction</h2> You can
 * create ranges using the provided static factory methods.
 * <ul>
 * <li>Create a single point range using the {@link #point(P)} method.</li>
 * <li>Create an inclusive (closed) range using the {@link #inclusive(P,P)}
 * method.</li>
 * <li>Create an exclusive (open) range using the {@link #exclusive(P)} method.</li>
 * <li>Create a range that is inclusive on the left and exclusive on the right
 * using the {@link #inclusiveExclusive(P,P)} method.</li>
 * <li>Create a range that is exclusive on the left and inclusive on the right
 * using the {@link #exclusiveInclusive(P,P)} method.</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
@Immutable
public final class Range implements Comparable<Range> {

    /**
     * Return a {@link Range} that spans every possible {@link Value}.
     * 
     * @return the Range
     */
    public static Range all() {
        return ALL;
    }

    /**
     * Return a {@link Range} that is GREATER_THAN_OR_EQUALS with respect to
     * {@code value}.
     * 
     * @param value
     * @return the Range
     */
    public static Range atLeast(Value value) {
        return inclusiveExclusive(value, Value.POSITIVE_INFINITY);
    }

    /**
     * Return a {@link Range} that is LESS_THAN_OR_EQUALS with respect to
     * {@code value}.
     * 
     * @param value
     * @return the Range
     */
    public static Range atMost(Value value) {
        return exclusiveInclusive(Value.NEGATIVE_INFINITY, value);
    }

    /**
     * Return a Range that conforms to the Concourse definition of
     * {@link Operator#BETWEEN} where the low value is included, but the high
     * value is excluded.
     * 
     * @param low
     * @param high
     * @return the Range
     */
    public static Range between(Value low, Value high) {
        return inclusiveExclusive(low, high);
    }

    /**
     * Return a Range that excludes both the left and right endpoints:
     * <em>(x, y)</em>. This is also known as a closed interval.
     * 
     * @param low
     * @param high
     * @return the Range
     */
    public static Range exclusive(Value low, Value high) {
        return new Range(new Endpoint(LowBound.EXCLUDE, low), new Endpoint(
                high, HighBound.EXCLUDE));
    }

    /**
     * Return a Range that excludes the left endpoint and includes the right
     * one: <em>(x, y]</em>. This is also known as a left-closed, right-open
     * interval.
     * 
     * @param low
     * @param high
     * @return the Range
     */
    public static Range exclusiveInclusive(Value low, Value high) {
        return new Range(new Endpoint(LowBound.EXCLUDE, low), new Endpoint(
                high, HighBound.INCLUDE));
    }

    /**
     * Return a {@link Range} that is strictly GREATER_THAN with respect to
     * {@code value}.
     * 
     * @param value
     * @return the Range
     */
    public static Range greaterThan(Value value) {
        return exclusive(value, Value.POSITIVE_INFINITY);
    }

    /**
     * Return a Range that includes both the left and right endpoints:
     * <em>[x, y]</em>. This is also known as a closed interval.
     * 
     * @param low
     * @param high
     * @return the Range
     */
    public static Range inclusive(Value low, Value high) {
        return new Range(new Endpoint(LowBound.INCLUDE, low), new Endpoint(
                high, HighBound.INCLUDE));
    }

    /**
     * Return a Range that includes the left endpoint and excludes the right
     * one: <em>[x, y)</em>. This is also known as a left-closed, right-open
     * interval.
     * 
     * @param low
     * @param high
     * @return the Range
     */
    public static Range inclusiveExclusive(Value low, Value high) {
        return new Range(new Endpoint(LowBound.INCLUDE, low), new Endpoint(
                high, HighBound.EXCLUDE));
    }

    /**
     * Return a {@link Range} that is strictly LESS_THAN with respect to
     * {@code value}.
     * 
     * @param value
     * @return the Range
     */
    public static Range lessThan(Value value) {
        return exclusive(Value.NEGATIVE_INFINITY, value);
    }

    /**
     * Return a Range that represents a single point: <em>[x, x]</em> or
     * {@code [x]}.
     * 
     * @param point
     * @return the Range
     */
    public static Range point(Value point) {
        return new Range(new Endpoint(LowBound.INCLUDE, point), new Endpoint(
                point, HighBound.INCLUDE));
    }

    /**
     * This is a Range that spans every possible {@link Value}.
     */
    private static Range ALL = Range.inclusive(Value.NEGATIVE_INFINITY,
            Value.POSITIVE_INFINITY);

    /**
     * A comparator that can be used to sort bounds.
     */
    private static final Comparator<Bound> BOUND_COMPARATOR = new Comparator<Bound>() {

        @Override
        public int compare(Bound o1, Bound o2) {
            return Integer.compare(o1.getValue(), o2.getValue());
        }

    };

    /**
     * The end of the range.
     */
    private final Endpoint high;

    /**
     * The start of the range.
     */
    private final Endpoint low;

    /**
     * Construct a new instance.
     * 
     * @param low.bound
     * @param left
     * @param right
     * @param high.bound
     */
    private Range(Endpoint low, Endpoint high) {
        this.low = low;
        this.high = high;
    }

    /**
     * 
     * <strong>This method is deprecated because it assumes a natural ordering
     * of Ranges that isn't valid. Use the comparators in the {@link Ranges}
     * class instead because those offer more specified semantics.</strong>
     * </p> {@inheritDoc}
     * 
     * @param other
     * @return
     */
    @Override
    @Deprecated
    public int compareTo(Range other) {
        return Ranges.lowThenHighValueComparator().compare(this, other);
    }

    /**
     * Return {@code true} if this range contains the {@code other} range. This
     * range is considered to contain the {@code other} range if the bounds of
     * the {@code other} one are completely within the bounds of this range.
     * 
     * @param other
     * @return {@code true} of this range contains the other
     */
    public boolean contains(Range other) {
        return compareToLeft(other) <= 0 && compareToRight(other) >= 0;
    }

    @Override
    public boolean equals(Object object) {
        if(object instanceof Range) {
            Range other = (Range) object;
            return Objects.equals(low.value, other.low.value)
                    && Objects.equals(high.value, other.high.value)
                    && Objects.equals(low.bound, other.low.bound)
                    && Objects.equals(high.bound, other.high.bound);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(low.value, high.value, low.bound, high.bound);
    }

    /**
     * Return a new {@link Range} that represents the intersection of this and
     * the {@code other} range or {@code null} if no such range exists.
     * 
     * @param other
     * @return the intersection range or {@code null} if this range does not
     *         intersect the {@code other} one.
     */
    @Nullable
    public Range intersection(Range other) {
        if(intersects(other)) {
            boolean iStart = compareToLeft(other) > 0;
            boolean iEnd = compareToRight(other) < 0;
            Range intersection = new Range(new Endpoint(iStart ? low.bound
                    : other.low.bound, iStart ? low.value : other.low.value),
                    new Endpoint(iEnd ? high.value : other.high.value,
                            iEnd ? high.bound : other.high.bound));
            return intersection.isEmpty() ? null : intersection;
        }
        else {
            return null;
        }
    }

    /**
     * Return {@code true} if this range intersects the {@code other} range.
     * This range is considered to intersect another one if the left endpoint of
     * this range is less than or equal to the right endpoint of the other range
     * and the right endpoint of this range is greater than or equal to the left
     * endpoint of the other range.
     * 
     * @param other
     * @return {@code true} if this range intersects the other
     */
    public boolean intersects(Range other) {
        return compareToRightLeft(other) >= 0 && compareToLeftRight(other) <= 0;
    }

    /**
     * Return {@code true} if this range only contains a single point.
     * 
     * @return {@code true} if this is a point
     */
    public boolean isPoint() {
        return low.value.equals(high.value);
    }

    public Range getLowPoint() {
        return new Range(new Endpoint(low.bound, low.value), new Endpoint(
                low.value, low.bound));
    }

    public Range getHighPoint() {
        return new Range(new Endpoint(high.bound, high.value), new Endpoint(
                high.value, high.bound));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(low.bound);
        sb.append(low.value);
        if(!low.value.equals(high.value)) {
            sb.append(",");
            sb.append(high.value);
        }
        sb.append(high.bound);
        return sb.toString();
    }

    /**
     * Return the set of ranges that include the points that are in either this
     * range or the {@code other} one and not in their intersection. The return
     * set will include between 0 and 2 ranges that together include all the
     * points that meet this criteria.
     * 
     * @param other
     * @return the set or ranges that make uValue the symmetric difference
     *         between
     *         this range and the {@code other} one
     */
    public Set<Range> xor(Range other) {
        Set<Range> ranges = Sets.newLinkedHashSet();
        Range intersection = intersection(other);
        if(intersection != null) {
            boolean iStart = compareToLeft(other) < 0;
            boolean iEnd = compareToRight(other) > 0;
            Range first = new Range(new Endpoint(iStart ? low.bound
                    : other.low.bound, iStart ? low.value : other.low.value),
                    new Endpoint(intersection.low.value, HighBound
                            .byValue(intersection.low.bound.getValue())));
            Range second = new Range(new Endpoint(
                    LowBound.byValue(intersection.high.bound.getValue()),
                    intersection.high.value), new Endpoint(iEnd ? high.value
                    : other.high.value, iEnd ? high.bound : other.high.bound));
            if(!first.isEmpty()) {
                ranges.add(first);
            }
            if(!second.isEmpty()) {
                ranges.add(second);
            }
        }
        return ranges;
    }

    /**
     * Return {@code true} if the range does not contain any points.
     * 
     * @return {@code true} if this is an empty range
     */
    private boolean isEmpty() {
        return isPoint() && low.bound.getValue() == high.bound.getValue();
    }

    /**
     * Compare the left endpoint of this range to the left endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    public int compareToLeft(Range other) {
        return ComparisonChain.start().compare(low.value, other.low.value)
                .compare(low.bound, other.low.bound, BOUND_COMPARATOR).result();
    }

    /**
     * Compare the left endpoint of this range with the right endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    protected int compareToLeftRight(Range other) {
        int c = low.value.compareTo(other.high.value);
        if(c == 0) {
            if(high.bound.getValue() + other.low.bound.getValue() == 1) { // bounds
                                                                          // are
                                                                          // the
                                                                          // "same"
                return 0;
            }
            else if(low.bound == LowBound.INCLUDE) {
                return 1;
            }
            else {
                return -1;
            }
        }
        else {
            return c;
        }
    }

    /**
     * Compare the right endpoint of this range to the right endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    public int compareToRight(Range other) {
        return ComparisonChain.start().compare(high.value, other.high.value)
                .compare(high.bound, other.high.bound, BOUND_COMPARATOR)
                .result();
    }

    /**
     * Compare the right endpoint of this range with the left endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    public int compareToRightLeft(Range other) {
        int c = high.value.compareTo(other.low.value);
        if(c == 0) {
            if(high.bound.getValue() + other.low.bound.getValue() == 1) { // bounds
                                                                          // are
                                                                          // the
                                                                          // "same"
                return 0;
            }
            else if(high.bound == HighBound.INCLUDE) {
                return 1;
            }
            else {
                return -1;
            }
        }
        else {
            return c;
        }
    }

    /**
     * An {@link Endpoint} is used to represent the low or high value in a
     * {@link Range}. Each has a value and a {@link Bound} that determines
     * whether the value is included or not.
     * 
     * @author jnelson
     */
    private static class Endpoint implements Comparable<Endpoint> {

        private final Bound bound;

        private final Value value;

        /**
         * Construct a new instance.
         * 
         * @param bound
         * @param value
         */
        private Endpoint(Bound bound, Value value) {
            this.value = value;
            this.bound = bound;
        }

        /**
         * Construct a new instance.
         * 
         * @param value
         * @param bound
         */
        private Endpoint(Value value, Bound bound) {
            this.value = value;
            this.bound = bound;
        }

        @Override
        public int compareTo(Endpoint other) {
            return ComparisonChain.start().compare(value, other.value)
                    .compare(bound, other.bound, BOUND_COMPARATOR).result();
        }

    }

    /**
     * The interface that all bracket enum types extend.
     * 
     * @author jnelson
     */
    interface Bound {
        /**
         * Return the value of the endpoint for sorting purposes.
         * 
         * @return the value
         */
        int getValue();
    }

    /**
     * Describes whether the right bound is included in the range or not. A
     * range with a closed (included) right endpoint X is considered
     * "greater than" a range with an open (excluded) right endpoint X (e.g. [X,
     * 4] is greater than [X,4) because the former ends at 4 whereas the latter
     * ends at 3.9999999999...9).
     * 
     * @author jnelson
     */
    enum HighBound implements Bound {
        EXCLUDE(0), INCLUDE(1);

        /**
         * Get the enum by its value.
         * 
         * @param value
         * @return the high.bound
         */
        static HighBound byValue(int value) {
            if(value == 0) {
                return EXCLUDE;
            }
            else {
                return INCLUDE;
            }
        }

        /**
         * The numerical value of the enum for calculation purposes.
         */
        final int value;

        /**
         * Construct a new instance.
         * 
         * @param value
         */
        HighBound(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            if(this == INCLUDE) {
                return "]";
            }
            else {
                return ")";
            }
        }
    }

    /**
     * Describes whether the left bound is included in the range or not. A range
     * with closed (included) left endpoint X is considered "less than" a range
     * with an open (excluded) left endpoint X (e.g. [1, Y] is less than (1, Y]
     * because the former starts at 1 wheras the latter starts at
     * 1.0000000000...1).
     * 
     * @author jnelson
     */
    enum LowBound implements Bound {
        EXCLUDE(1), INCLUDE(0);

        /**
         * Get the enum by its value.
         * 
         * @param value
         * @return the low.bound
         */
        static LowBound byValue(int value) {
            if(value == 0) {
                return INCLUDE;
            }
            else {
                return EXCLUDE;
            }
        }

        /**
         * The numerical value of the enum for calculation purposes.
         */
        final int value;

        /**
         * Construct a new instance.
         * 
         * @param value
         */
        LowBound(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            if(this == INCLUDE) {
                return "[";
            }
            else {
                return "(";
            }
        }
    }
}
