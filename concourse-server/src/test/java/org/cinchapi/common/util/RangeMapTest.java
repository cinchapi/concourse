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

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests that check for conformity to the {@link RangeMap} interface.
 * 
 * @author jnelson
 */
public abstract class RangeMapTest extends ConcourseBaseTest {

    private RangeMap<Integer, String> map;

    @Override
    protected void beforeEachTest() {
        super.beforeEachTest();
        map = getRangeMap();
    }

    /**
     * Return a {@link RangeMap} to use within the unit tests.
     * 
     * @return the RangeMap
     */
    protected abstract RangeMap<Integer, String> getRangeMap();

    @Test
    public void testContainsRange() {
        Range<Integer> a = Range.inclusive(1, 10);
        Range<Integer> b = Range.inclusive(5, 20);
        map.put(a, TestData.getString());
        Assert.assertTrue(map.contains(b));
    }

    @Test
    public void testDoesNotContainRange() {
        Range<Integer> a = Range.inclusive(1, 10);
        Range<Integer> b = Range.exclusiveInclusive(10, 20);
        map.put(a, TestData.getString());
        Assert.assertFalse(map.contains(b));
    }

    @Test
    public void testGetRange() {
        Range<Integer> a = Range.inclusive(1, 10);
        Range<Integer> b = Range.inclusiveExclusive(5, 10);
        Range<Integer> c = Range.exclusive(30, 60);
        Range<Integer> d = Range.inclusive(1, 100);
        map.put(a, "a");
        map.put(b, "b");
        map.put(c, "c");
        map.put(d, "d");
        Assert.assertEquals(Sets.newHashSet("c", "d"),
                map.get(Range.inclusive(50, 80)));
        Assert.assertEquals(Sets.newHashSet("a", "b", "c", "d"),
                map.get(Range.exclusive(1, 100)));
        Assert.assertEquals(Sets.newHashSet("a", "d"),
                map.get(Range.inclusive(10, 30)));
    }

}
