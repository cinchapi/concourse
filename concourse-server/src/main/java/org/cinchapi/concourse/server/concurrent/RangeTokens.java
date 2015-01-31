package org.cinchapi.concourse.server.concurrent;

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
