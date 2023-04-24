/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.model;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Range;

/**
 * Unit tests for {@link Ranges}.
 *
 * @author Jeff Nelson
 */
public class RangesTest {

    @Test
    public void testHaveNonEmptyIntersection() {
        Range<Value> a = randomRange();
        Range<Value> b = randomRange();
        System.out.println(a);
        System.out.println(b);
        boolean expected;
        try {
            expected = !a.intersection(b).isEmpty();
        }
        catch (IllegalArgumentException e) {
            expected = false;
        }
        Assert.assertEquals(expected, Ranges.haveNonEmptyIntersection(a, b));
    }

    @Test
    public void testHaveNonEmptyIntersectionReproA() {
        Range<Value> a = Range.closedOpen(
                Value.wrap(Convert.javaToThrift(-1307906851)),
                Value.wrap(Convert.javaToThrift(740542968)));
        Range<Value> b = Range.closedOpen(
                Value.wrap(Convert.javaToThrift(255904862)),
                Value.wrap(Convert.javaToThrift(1010153814)));
        boolean expected;
        try {
            expected = !a.intersection(b).isEmpty();
        }
        catch (IllegalArgumentException e) {
            expected = false;
        }
        Assert.assertEquals(expected, Ranges.haveNonEmptyIntersection(a, b));
    }

    @Test
    public void testHaveNonEmptyIntersectionReproB() {
        Range<Value> a = Range.closedOpen(
                Value.wrap(Convert.javaToThrift(-14136856)),
                Value.wrap(Convert.javaToThrift(1519386470)));
        Range<Value> b = Range.closed(
                Value.wrap(Convert.javaToThrift(879784252)),
                Value.wrap(Convert.javaToThrift(879784252)));
        boolean expected;
        try {
            expected = !a.intersection(b).isEmpty();
        }
        catch (IllegalArgumentException e) {
            expected = false;
        }
        Assert.assertEquals(expected, Ranges.haveNonEmptyIntersection(a, b));
    }

    @Test
    public void testHaveNonEmptyIntersectionReproC() {
        Range<Value> a = Range.closedOpen(
                Value.wrap(Convert.javaToThrift(-2114795584)),
                Value.wrap(Convert.javaToThrift(2012556173)));
        Range<Value> b = Range.closed(
                Value.wrap(Convert.javaToThrift(1307191844)),
                Value.wrap(Convert.javaToThrift(1307191844)));
        boolean expected;
        try {
            expected = !a.intersection(b).isEmpty();
        }
        catch (IllegalArgumentException e) {
            expected = false;
        }
        Assert.assertEquals(expected, Ranges.haveNonEmptyIntersection(a, b));
    }

    /**
     * Return a random {@link Range}.
     * 
     * @return the {@link Range}
     */
    private static Range<Value> randomRange() {
        int a = TestData.getInt();
        int b = TestData.getInt();
        Value low = Value.wrap(Convert.javaToThrift(Math.min(a, b)));
        Value high = Value.wrap(Convert.javaToThrift(Math.max(a, b)));
        Text key = TestData.getText();
        int c = TestData.getScaleCount();
        RangeToken token;
        if(c % 3 == 0) {
            token = RangeToken.forReading(key, Operator.BETWEEN, low, high);
        }
        else if(c % 4 == 0) {
            boolean d = Numbers.isEven(TestData.getScaleCount());
            boolean e = Numbers.isEven(TestData.getScaleCount());
            Operator operator;
            if(d & e) {
                operator = Operator.GREATER_THAN_OR_EQUALS;
            }
            else if(d & !e) {
                operator = Operator.GREATER_THAN;
            }
            else if(!d & e) {
                operator = Operator.LESS_THAN_OR_EQUALS;
            }
            else {
                operator = Operator.LESS_THAN;
            }
            token = RangeToken.forReading(key, operator, low);
        }
        else {
            token = RangeToken.forWriting(key, high);
        }
        return token.ranges().iterator().next();
    }

}
