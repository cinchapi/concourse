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

import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkState;

/**
 * A {@link Range} is an interval between two points that refers to
 * all the values that lie in between. <h2>Range Construction</h2>
 * <p>
 * You can create ranges using the provided static factory methods.
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
public final class Range<P extends Comparable<P>> implements
        Comparable<Range<P>> {

    /**
     * Return a Range that excludes both the left and right endpoints:
     * <em>(x, y)</em>. This is also known as a closed interval.
     * 
     * @param left
     * @param right
     * @return the Range
     */
    public static <P extends Comparable<P>> Range<P> exclusive(P left, P right) {
        return new Range<P>(LeftEndpoint.EXCLUDE, left, right,
                RightEndpoint.EXCLUDE);
    }

    /**
     * Return a Range that excludes the left endpoint and includes the right
     * one: <em>(x, y]</em>. This is also known as a left-closed, right-open
     * interval.
     * 
     * @param left
     * @param right
     * @return the Range
     */
    public static <P extends Comparable<P>> Range<P> exclusiveInclusive(P left,
            P right) {
        return new Range<P>(LeftEndpoint.EXCLUDE, left, right,
                RightEndpoint.INCLUDE);
    }

    /**
     * Return a Range that includes both the left and right endpoints:
     * <em>[x, y]</em>. This is also known as a closed interval.
     * 
     * @param left
     * @param right
     * @return the Range
     */
    public static <P extends Comparable<P>> Range<P> inclusive(P left, P right) {
        return new Range<P>(LeftEndpoint.INCLUDE, left, right,
                RightEndpoint.INCLUDE);
    }

    /**
     * Return a Range that includes the left endpoint and excludes the right
     * one: <em>[x, y)</em>. This is also known as a left-closed, right-open
     * interval.
     * 
     * @param left
     * @param right
     * @return the Range
     */
    public static <P extends Comparable<P>> Range<P> inclusiveExclusive(P left,
            P right) {
        return new Range<P>(LeftEndpoint.INCLUDE, left, right,
                RightEndpoint.EXCLUDE);
    }

    /**
     * Return a Range that represents a single point: <em>[x, x]</em> or
     * {@code [x]}.
     * 
     * @param point
     * @return the Range
     */
    public static <P extends Comparable<P>> Range<P> point(P point) {
        return new Range<P>(LeftEndpoint.INCLUDE, point, point,
                RightEndpoint.INCLUDE);
    }

    /**
     * A comparator that can be used to sort endpoints.
     */
    private static final Comparator<Endpoint> ENDPOINT_COMPARATOR = new Comparator<Endpoint>() {

        @Override
        public int compare(Endpoint o1, Endpoint o2) {
            return Integer.compare(o1.getValue(), o2.getValue());
        }

    };

    /**
     * The start of the range.
     */
    private final P left;

    /**
     * Determines whether the Range is closed or open on the left.
     */
    private final LeftEndpoint leftEndpoint;

    /**
     * The end of the range.
     */
    private final P right;

    /**
     * Determines whether the Range is closed or open on the right.
     */
    private final RightEndpoint rightEndpoint;

    /**
     * Construct a new instance.
     * 
     * @param leftEndpoint
     * @param left
     * @param right
     * @param rightEndpoint
     */
    private Range(LeftEndpoint leftEndpoint, P left, P right,
            RightEndpoint rightEndpoint) {
        checkState(left.compareTo(right) <= 0,
                "Cannot create a range with a left value that is larger than the right");
        this.leftEndpoint = leftEndpoint;
        this.left = left;
        this.right = right;
        this.rightEndpoint = rightEndpoint;
    }

    @Override
    public int compareTo(Range<P> other) {
        // A total order can be defined on the intervals by ordering them first
        // by their 'low' value and finally by their 'high' value. This ordering
        // can be used to prevent duplicate intervals from being inserted into
        // the tree in O(log n) time, versus the O(k + log n) time required to
        // find duplicates if k intervals overlap a new interval. (source:
        // http://en.wikipedia.org/wiki/Interval_tree#Augmented_tree)
        return ComparisonChain.start().compare(left, other.left)
                .compare(leftEndpoint, other.leftEndpoint)
                .compare(right, other.right)
                .compare(rightEndpoint, other.rightEndpoint).result();
    }

    /**
     * Return {@code true} if this range contains the {@code other} range. This
     * range is considered to contain the {@code other} range if the bounds of
     * the {@code other} one are completely within the bounds of this range.
     * 
     * @param other
     * @return {@code true} of this range contains the other
     */
    public boolean contains(Range<P> other) {
        return compareToLeft(other) <= 0 && compareToRight(other) >= 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object object) {
        if(object instanceof Range) {
            Range<P> other = (Range<P>) object;
            return Objects.equals(left, other.left)
                    && Objects.equals(right, other.right)
                    && leftEndpoint == other.leftEndpoint
                    && rightEndpoint == other.rightEndpoint;
        }
        else {
            return false;
        }
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
    public Range<P> intersection(Range<P> other) {
        if(other.contains(Range.point(left))
                || other.contains(Range.point(right))
                || contains(Range.point(other.left))
                || contains(Range.point(other.right))) {
            boolean iStart = compareToLeft(other) > 0;
            boolean iEnd = compareToRight(other) < 0;
            Range<P> intersection = new Range<P>(iStart ? leftEndpoint
                    : other.leftEndpoint, iStart ? left : other.left,
                    iEnd ? right : other.right, iEnd ? rightEndpoint
                            : other.rightEndpoint);
            return intersection.isEmpty() ? null : intersection;
        }
        else {
            return null;
        }
    }

    /**
     * Return the set of ranges that include the points that are in either this
     * range or the {@code other} one and not in their intersection. The return
     * set will include between 0 and 2 ranges that together include all the
     * points that meet this criteria.
     * 
     * @param other
     * @return the set or ranges that make up the symmetric difference between
     *         this range and the {@code other} one
     */
    public Set<Range<P>> xor(Range<P> other) {
        Set<Range<P>> ranges = Sets.newLinkedHashSet();
        Range<P> intersection = intersection(other);
        if(intersection != null) {
            boolean iStart = compareToLeft(other) < 0;
            boolean iEnd = compareToRight(other) > 0;
            Range<P> first = new Range<P>(iStart ? leftEndpoint
                    : other.leftEndpoint, iStart ? left : other.left,
                    intersection.left,
                    RightEndpoint.byValue(intersection.leftEndpoint.value));
            Range<P> second = new Range<P>(
                    LeftEndpoint.byValue(intersection.rightEndpoint.value),
                    intersection.right, iEnd ? right : other.right,
                    iEnd ? rightEndpoint : other.rightEndpoint);
            if(!first.isEmpty()) {
                ranges.add(first);
            }
            if(!second.isEmpty()) {
                ranges.add(second);
            }
        }
        return ranges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, leftEndpoint, rightEndpoint);
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
    public boolean intersects(Range<P> other) {
        return intersection(other) != null;
    }

    /**
     * Return {@code true} if this range only contains a single point.
     * 
     * @return {@code true} if this is a point
     */
    public boolean isPoint() {
        return left.equals(right);
    }

    /**
     * Return {@code true} if the range does not contain any points.
     * 
     * @return {@code true} if this is an empty range
     */
    private boolean isEmpty() {
        return isPoint() && leftEndpoint.value == rightEndpoint.value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftEndpoint);
        sb.append(left);
        if(!left.equals(right)) {
            sb.append(",");
            sb.append(right);
        }
        sb.append(rightEndpoint);
        return sb.toString();
    }

    /**
     * Compare the left endpoint of this range to the left endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    protected int compareToLeft(Range<P> other) {
        return ComparisonChain.start().compare(left, other.left)
                .compare(leftEndpoint, other.leftEndpoint, ENDPOINT_COMPARATOR)
                .result();
    }

    /**
     * Compare the left endpoint of this range with the right endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    protected int compareToLeftRight(Range<P> other) {
        int c = left.compareTo(other.right);
        if(c == 0) {
            if(rightEndpoint.value + other.leftEndpoint.value == 1) { // endpoints
                                                                      // are
                                                                      // the
                                                                      // "same"
                return 0;
            }
            else if(leftEndpoint == LeftEndpoint.INCLUDE) {
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
    protected int compareToRight(Range<P> other) {
        return ComparisonChain
                .start()
                .compare(right, other.right)
                .compare(rightEndpoint, other.rightEndpoint,
                        ENDPOINT_COMPARATOR).result();
    }

    /**
     * Compare the right endpoint of this range with the left endpoint of the
     * {@code other} range.
     * 
     * @param other
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the other object.
     */
    protected int compareToRightLeft(Range<P> other) {
        int c = right.compareTo(other.left);
        if(c == 0) {
            if(rightEndpoint.value + other.leftEndpoint.value == 1) { // endpoints
                                                                      // are
                                                                      // the
                                                                      // "same"
                return 0;
            }
            else if(rightEndpoint == RightEndpoint.INCLUDE) {
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
     * The interface that all endpoint enum types extend.
     * 
     * @author jnelson
     */
    interface Endpoint {
        /**
         * Return the value of the endpoint for sorting purposes.
         * 
         * @return the value
         */
        int getValue();
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
    enum LeftEndpoint implements Endpoint {
        EXCLUDE(1), INCLUDE(0);

        /**
         * Get the enum by its value.
         * 
         * @param value
         * @return the LeftEndpoint
         */
        static LeftEndpoint byValue(int value) {
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
        LeftEndpoint(int value) {
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

    /**
     * Describes whether the right bound is included in the range or not. A
     * range with a closed (included) right endpoint X is considered
     * "greater than" a range with an open (excluded) right endpoint X (e.g. [X,
     * 4] is greater than [X,4) because the former ends at 4 whereas the latter
     * ends at 3.9999999999...9).
     * 
     * @author jnelson
     */
    enum RightEndpoint implements Endpoint {
        EXCLUDE(0), INCLUDE(1);

        /**
         * Get the enum by its value.
         * 
         * @param value
         * @return the RightEndpoint
         */
        static RightEndpoint byValue(int value) {
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
        RightEndpoint(int value) {
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
}
