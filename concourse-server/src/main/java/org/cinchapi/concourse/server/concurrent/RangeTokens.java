package org.cinchapi.concourse.server.concurrent;

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
import java.util.List;

import org.cinchapi.common.util.Range;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;

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

}
