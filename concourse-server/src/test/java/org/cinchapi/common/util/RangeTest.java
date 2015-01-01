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

import java.util.TreeSet;

import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link Range} data type.
 * 
 * @author jnelson
 */
public class RangeTest {

    @Test
    public void testPointEquality() {
        int num = TestData.getScaleCount();
        Range<Integer> x = Range.point(num);
        Range<Integer> y = Range.point(num);
        Assert.assertEquals(x, y);
    }

    @Test
    public void testInclusiveRangeNotEqualToExclusiveRange() {
        int left = TestData.getScaleCount();
        int right = left + TestData.getScaleCount();
        Range<Integer> ex = Range.exclusive(left, right);
        Range<Integer> in = Range.inclusive(left, right);
        Assert.assertNotEquals(ex, in);
    }

    @Test
    public void testInclusiveExclusiveRangeNotEqualToExclusiveInclusiveRange() {
        int left = TestData.getScaleCount();
        int right = left + TestData.getScaleCount();
        Range<Integer> ex = Range.inclusiveExclusive(left, right);
        Range<Integer> in = Range.exclusiveInclusive(left, right);
        Assert.assertNotEquals(ex, in);
    }

    @Test
    public void testCompareToRightLeftWithEqualValuesMeIncludedOtherExcluded() {
        int num = TestData.getScaleCount();
        Range<Integer> my = Range
                .inclusive(num - TestData.getScaleCount(), num);
        Range<Integer> other = Range.exclusive(num,
                num + TestData.getScaleCount());
        Assert.assertTrue(my.compareToRightLeft(other) > 0);
    }

    @Test
    public void testCompareToLeftRightWithEqualValuesMeIncludedOtherExcluded() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .inclusive(num, num + TestData.getScaleCount());
        Range<Integer> other = Range.exclusive(num - TestData.getScaleCount(),
                num);
        Assert.assertTrue(me.compareToLeftRight(other) > 0);
    }

    @Test
    public void testCompareToRightLeftWithEqualValuesMeExcludedOtherIncluded() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .exclusive(num - TestData.getScaleCount(), num);
        Range<Integer> other = Range.inclusive(num,
                num + TestData.getScaleCount());
        Assert.assertTrue(me.compareToRightLeft(other) < 0);
    }

    @Test
    public void testCompareToLeftRightWithEqualValuesMeExcludedOtherIncluded() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .exclusive(num, num + TestData.getScaleCount());
        Range<Integer> other = Range.inclusive(num - TestData.getScaleCount(),
                num);
        Assert.assertTrue(me.compareToLeftRight(other) < 0);
    }

    @Test
    public void testCompareToRightLeftWithEqualValuesAndExclusiveEndpoints() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .exclusive(num - TestData.getScaleCount(), num);
        Range<Integer> other = Range.exclusive(num,
                num + TestData.getScaleCount());
        Assert.assertTrue(me.compareToRightLeft(other) == 0);
    }

    @Test
    public void testCompareToLeftRightWithEqualValuesAndExclusiveEndpoints() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .exclusive(num, num + TestData.getScaleCount());
        Range<Integer> other = Range.exclusive(num - TestData.getScaleCount(),
                num);
        Assert.assertTrue(me.compareToLeftRight(other) == 0);
    }

    @Test
    public void testCompareToRightLeftWithEqualValuesAndInclusiveEndpoints() {
        int num = TestData.getScaleCount();
        Range<Integer> me = Range
                .inclusive(num - TestData.getScaleCount(), num);
        Range<Integer> other = Range.inclusive(num,
                num + TestData.getScaleCount());
        Assert.assertTrue(me.compareToRightLeft(other) == 0);
    }

    @Test
    public void testCompareToRightLeftWithNonEqualValues() {
        int num1 = TestData.getScaleCount();
        int num2 = num1 + TestData.getScaleCount()
                * (Numbers.isEven(TestData.getInt()) ? 1 : -1);
        Range<Integer> me = Range.inclusive(num1 - TestData.getScaleCount(),
                num1);
        Range<Integer> other = Range.inclusive(num2,
                num2 + TestData.getScaleCount());
        Assert.assertEquals(Integer.compare(num1, num2),
                me.compareToRightLeft(other));
    }
    
    @Test
    public void testContains(){
        Range<Integer> a = Range.inclusive(1, 10);
        Range<Integer> b = Range.inclusive(-1, 11);
        Assert.assertFalse(a.contains(b));
        Assert.assertTrue(b.contains(a));
        Assert.assertTrue(a.contains(a));
    }
    
    @Test
    public void testContainsExclusive(){
        Range<Integer> a = Range.exclusive(1, 10);
        Range<Integer> b = Range.exclusive(1, 10);
        Assert.assertTrue(a.contains(b));
        
        b = Range.exclusive(2, 10);
        Assert.assertTrue(a.contains(b));
        
        b = Range.exclusiveInclusive(1, 10);
        Assert.assertFalse(a.contains(b));
    }
    
    @Test
    public void testContainsPoint(){
        Range<Integer> a = Range.inclusive(10, 30);
        Range<Integer> b = Range.point(26);
        Assert.assertTrue(a.contains(b));
        Assert.assertFalse(b.contains(a));
    }
    
    @Test
    public void testContainsInclusiveInclusive(){
        Range<Integer> a = Range.inclusive(8, 20);
        Range<Integer> b = Range.inclusive(1,10);
        Assert.assertFalse(a.contains(b));   
    }
    
    @Test
    public void testContainedRangeAlwaysIntersects(){
        TreeSet<Integer> set = Sets.newTreeSet();
        while(set.size() < 4){
            set.add(TestData.getInt());
        }
        Range<Integer> a = Range.inclusive(Iterables.get(set, 0), Iterables.get(set, 3));
        Range<Integer> b = Range.inclusive(Iterables.get(set, 1), Iterables.get(set, 2));
        Assert.assertTrue(a.contains(b));
        Assert.assertTrue(a.intersects(b));
    }

}
