/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.concurrent.RangeTokens}.
 *
 * @author Jeff Nelson
 */
public class RangeTokenTest extends ConcourseBaseTest {

    @Test
    public void testEqualRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range, Range.singleton(value));
    }

    @Test
    public void testWriteRanges() {
        Text key = TestData.getText();
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forWriting(key, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range, Range.singleton(value));
    }

    @Test
    public void testNotEqualRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.NOT_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Iterable<Range<Value>> ranges = token.ranges();
        Assert.assertEquals(Lists.newArrayList(Range.lessThan(value),
                Range.greaterThan(value)), ranges);
    }

    @Test
    public void testGreaterThanRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.GREATER_THAN;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range, Range.greaterThan(value));
    }

    @Test
    public void testGreaterThanOrEqualsRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.GREATER_THAN_OR_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range, Range.atLeast(value));
    }

    @Test
    public void testLessThanRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.LESS_THAN;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());;
        Assert.assertEquals(range, Range.lessThan(value));
    }

    @Test
    public void testLessThanOrEqualsRanges() {
        Text key = TestData.getText();
        Operator operator = Operator.LESS_THAN_OR_EQUALS;
        Value value = TestData.getValue();
        RangeToken token = RangeToken.forReading(key, operator, value);
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range, Range.atMost(value));
    }

    @Test
    public void testBetweenRanges() {
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
        Range<Value> range = Iterables.getOnlyElement(token.ranges());
        Assert.assertEquals(range,
                Range.closedOpen(value1.compareTo(value2) < 0 ? value1 : value2,
                        value1.compareTo(value2) > 0 ? value1 : value2));
    }

}
