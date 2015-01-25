package org.cinchapi.concourse.server.concurrent;

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
import java.util.Comparator;
import java.util.List;

import org.cinchapi.common.util.Range;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 * A collection of utility functions for dealing with {@link RangeToken}
 * objects.
 * 
 * @author jnelson
 */
public final class RangeTokens {

    /**
     * Convert the {@code rangeToken} to the analogous {@link Range} object.
     * 
     * @param rangeToken
     * @return the Range
     */
    public static Iterable<Range<Value>> convertToRange(RangeToken rangeToken) {
        List<Range<Value>> ranges = Lists.newArrayListWithCapacity(2);
        if(rangeToken.getOperator() == Operator.EQUALS
                || rangeToken.getOperator() == Operator.REGEX
                || rangeToken.getOperator() == null) { // null operator means
                                                       // the range token is for
                                                       // writing
            ranges.add(Range.point(rangeToken.getValues()[0]));
        }
        else if(rangeToken.getOperator() == Operator.NOT_EQUALS
                || rangeToken.getOperator() == Operator.NOT_REGEX) {
            ranges.add(Range.exclusive(Value.NEGATIVE_INFINITY,
                    rangeToken.getValues()[0]));
            ranges.add(Range.exclusive(rangeToken.getValues()[0],
                    Value.POSITIVE_INFINITY));
        }
        else if(rangeToken.getOperator() == Operator.GREATER_THAN) {
            ranges.add(Range.exclusive(rangeToken.getValues()[0],
                    Value.POSITIVE_INFINITY));
        }
        else if(rangeToken.getOperator() == Operator.GREATER_THAN_OR_EQUALS) {
            ranges.add(Range.inclusiveExclusive(rangeToken.getValues()[0],
                    Value.POSITIVE_INFINITY));
        }
        else if(rangeToken.getOperator() == Operator.LESS_THAN) {
            ranges.add(Range.exclusive(Value.NEGATIVE_INFINITY,
                    rangeToken.getValues()[0]));
        }
        else if(rangeToken.getOperator() == Operator.LESS_THAN_OR_EQUALS) {
            ranges.add(Range.exclusiveInclusive(Value.NEGATIVE_INFINITY,
                    rangeToken.getValues()[0]));
        }
        else if(rangeToken.getOperator() == Operator.BETWEEN) {
            ranges.add(Range.inclusiveExclusive(rangeToken.getValues()[0],
                    rangeToken.getValues()[1]));
        }
        else {
            throw new UnsupportedOperationException();
        }
        return ranges;
    }

    /**
     * A comparator that gives an approximate sorting or range tokens based on
     * their values. This comparator rarely says range tokens are equal (e.g.
     * equality is pretty much limited to cases when the tokens have the same
     * number of values, each of which are equal. Generally speaking, this
     * comparator will seek to sort RangeTokens in the way that they would
     * appear on a number line.
     * <p>
     * <strong>WARNING:</strong> This comparator intentionally considers range
     * tokens that represent an EQUAL read (e.g. key = foo) to be the same as
     * range tokens that represent a matching write (e.g. key AS foo). Ensure
     * that this feature is acceptable for your use case before sorting with
     * this comparator.
     * </p>
     * <p>
     * <strong>NOTE:</strong> This comparator does not take the {@code key}
     * component of the RangeToken into account!!!
     * </p>
     */
    public static Comparator<RangeToken> APPROX_VALUE_COMPARATOR = new Comparator<RangeToken>() {

        @Override
        public int compare(RangeToken arg0, RangeToken arg1) {
            List<Range<Value>> ranges0 = Lists
                    .newArrayList(convertToRange(arg0));
            List<Range<Value>> ranges1 = Lists
                    .newArrayList(convertToRange(arg1));
            if(ranges0.size() == ranges1.size()) {
                ComparisonChain chain = ComparisonChain.start();
                for (int i = 0; i < ranges0.size(); i++) {
                    chain = chain.compare(ranges0.get(i), ranges1.get(i));
                }
                chain = chain.compare(arg0.getOperator() == null ? -1 : arg0.getOperator().ordinal(), arg1.getOperator() == null ? -1 : arg1.getOperator().ordinal());
                return chain.result();
            }
            else if(ranges0.size() > 1) {
                for (Range<Value> range : ranges0) {
                    if(range.compareTo(ranges1.get(0)) < 0) {
                        return -1;
                    }
                }
                return 1;
            }
            else { // ranges1.size() > 1
                for (Range<Value> range : ranges1) {
                    if(range.compareTo(ranges0.get(0)) < 0) {
                        return -1;
                    }
                }
                return 1;
            }
        }

    };

}
