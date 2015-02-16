package org.cinchapi.concourse.server.concurrent;

import java.util.List;

import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Range;

import com.google.common.collect.Lists;

/**
 * A collection of utility functions for dealing with {@link RangeToken}
 * objects.
 * 
 * @author jnelson
 */
public final class RangeTokens {

    /**
     * Convert the {@code rangeToken} to the analogous {@link Range} objects.
     * <p>
     * NOTE: This method returns an iterable because a NOT_EQUALS RangeToken is
     * split into two discreet ranges. If you know the RangeToken does not have
     * a NOT_EQUALS operator, you can assume that the returned Iterable only has
     * one element.
     * </p>
     * 
     * @param token
     * @return the Range
     */
    public static Iterable<Range> convertToRange(RangeToken token) {
        List<Range> ranges = Lists.newArrayListWithCapacity(1);
        if(token.getOperator() == Operator.EQUALS
                || token.getOperator() == null) { // null operator means
                                                  // the range token is for
                                                  // writing
            ranges.add(Range.point(token.getValues()[0]));
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
            ranges.add(Range.lessThan(Value.NEGATIVE_INFINITY));
        }
        else if(token.getOperator() == Operator.LESS_THAN_OR_EQUALS) {
            ranges.add(Range.atMost(token.getValues()[0]));
        }
        else if(token.getOperator() == Operator.BETWEEN) {
            Value a = token.getValues()[0];
            Value b = token.getValues()[1];
            if(a == Value.NEGATIVE_INFINITY && b == Value.POSITIVE_INFINITY) {
                ranges.add(Range.all());
            }
            else {
                ranges.add(Range.between(a, b));
            }
        }
        else if(token.getOperator() == Operator.REGEX
                || token.getOperator() == Operator.NOT_REGEX) {
            ranges.add(Range.all());
        }
        else {
            throw new UnsupportedOperationException();
        }
        return ranges;
    }

}