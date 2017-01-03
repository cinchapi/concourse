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
package com.cinchapi.concourse.server.concurrent;

import java.util.List;

import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Range;
import com.google.common.collect.Lists;

/**
 * A collection of utility functions for dealing with {@link RangeToken}
 * objects.
 * 
 * @author Jeff Nelson
 */
public final class RangeTokens {

    /**
     * Convert the specified range {@code token} to one or more {@link Range
     * ranges} that provide the appropriate coverage.
     * 
     * @param token
     * @return the Ranges
     */
    public static Iterable<Range<Value>> convertToRange(RangeToken token) {
        List<Range<Value>> ranges = Lists.newArrayListWithCapacity(1);
        if(token.getOperator() == Operator.EQUALS
                || token.getOperator() == null) { // null operator means
                                                  // the range token is for
                                                  // writing
            ranges.add(Range.singleton(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.NOT_EQUALS) {
            ranges.add(Range.lessThan(token.getValues()[0]));
            ranges.add(Range.greaterThan(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.GREATER_THAN) {
            ranges.add(Range.greaterThan(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.GREATER_THAN_OR_EQUALS) {
            ranges.add(Range.atLeast(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.LESS_THAN) {
            ranges.add(Range.lessThan(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.LESS_THAN_OR_EQUALS) {
            ranges.add(Range.atMost(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.BETWEEN) {
            Value a = token.getValues()[0];
            Value b = token.getValues()[1];
            if(a == Value.NEGATIVE_INFINITY && b == Value.POSITIVE_INFINITY) {
                ranges.add(Range.<Value> all());
            }
            else if(token.getValues().length == 3) {
                ranges.add(Range.open(a, b));
            }
            else if(token.getValues().length == 4) {
                ranges.add(Range.closed(a, b));
            }
            else if(token.getValues().length == 5) {
                ranges.add(Range.openClosed(a, b));
            }
            else {
                ranges.add(Range.closedOpen(a, b));
            }
        }
        else if(token.getOperator() == Operator.REGEX
                || token.getOperator() == Operator.NOT_REGEX) {
            ranges.add(Range.<Value> all());
        }
        else {
            throw new UnsupportedOperationException();
        }
        return ranges;
    }

}
