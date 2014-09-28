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
package org.cinchapi.concourse.server.concurrent;

import java.util.Set;

import org.cinchapi.common.util.Range;
import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link RangeTokens}.
 * 
 * @author jnelson
 */
public class RangeTokensTest extends ConcourseBaseTest {

    @Test
    public void testConvertEqualRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertTrue(range.isPoint());
        Assert.assertEquals(range, Range.point(value));
    }

    @Test
    public void testConvertWriteRangeToken() {
        Text key = TestData.getText();
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forWriting(key, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertTrue(range.isPoint());
        Assert.assertEquals(range, Range.point(value));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConvertNotEqualRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.NOT_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Set<Range<Value>> ranges = RangeTokens.convertToRange(token);
        Assert.assertEquals(Sets.newHashSet(
                Range.exclusive(Value.NEGATIVE_INFINITY, value),
                Range.exclusive(value, Value.POSITIVE_INFINITY)), ranges);
    }

    @Test
    public void testConvertGreaterThanRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.GREATER_THAN;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertEquals(range,
                Range.exclusive(value, Value.POSITIVE_INFINITY));
    }

    @Test
    public void testConvertGreaterThanOrEqualsRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.GREATER_THAN_OR_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertEquals(range,
                Range.inclusiveExclusive(value, Value.POSITIVE_INFINITY));
    }

    @Test
    public void testConvertLessThanRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.LESS_THAN;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertEquals(range,
                Range.exclusive(Value.NEGATIVE_INFINITY, value));
    }

    @Test
    public void testConvertLessThanOrEqualsRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.LESS_THAN_OR_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertEquals(range,
                Range.exclusiveInclusive(Value.NEGATIVE_INFINITY, value));
    }

    @Test
    public void testConvertBetweenRangeToken() {
        Text key = TestData.getText();
        Operator operator = Operator.BETWEEN;
        Value value1 = TestData.getValue();
        Value value2 = null;
        while (value2 == null || value1.equals(value2)) {
            value2 = TestData.getValue();
        }
        RangeToken token = RangeToken.forReading(key, operator,
                value1.compareTo(value2) < 0 ? value1 : value2,
                value1.compareTo(value2) > 0 ? value1 : value2);
        Range<Value> range = Iterables.getOnlyElement(RangeTokens
                .convertToRange(token));
        Assert.assertEquals(range, Range.inclusiveExclusive(
                value1.compareTo(value2) < 0 ? value1 : value2,
                value1.compareTo(value2) > 0 ? value1 : value2));
    }

}
