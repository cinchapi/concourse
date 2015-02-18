package org.cinchapi.concourse.server.concurrent;

import java.util.List;

import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.collect.Range;
import com.google.common.collect.Lists;

/**
 * A collection of utility functions for dealing with {@link RangeToken}
 * objects.
 * 
 * @author jnelson
 */
public final class RangeTokens {

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
