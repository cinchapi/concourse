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
package org.cinchapi.concourse.util;

import java.util.Arrays;
import java.util.Set;

import org.cinchapi.concourse.time.Time;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link TSets} utility class.
 * 
 * @author jnelson
 */
public class TSetsTest {

    @Test
    public void testSortedIntersection() {
        Set<Integer> a = Sets.newTreeSet();
        Set<Integer> b = Sets.newTreeSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
    }

    @Test
    public void testSortedIntersectionReproCON_245() {
        Set<Long> a = Sets.newTreeSet(Arrays.<Long> asList(1424523725311000L,
                1424523725341000L, 1424523725370000L, 1424523725421002L,
                1424523725491000L, 1424523725514000L, 1424523725534000L,
                1424523725565000L, 1424523725584003L, 1424523725605000L,
                1424523725622002L, 1424523725641002L, 1424523725659002L,
                1424523725676004L, 1424523725691000L, 1424523725710000L,
                1424523725724000L, 1424523725738000L, 1424523725786002L,
                1424523725801000L, 1424523725823002L, 1424523725841000L,
                1424523725868000L, 1424523725895000L, 1424523725909000L,
                1424523725924000L, 1424523725937002L, 1424523725963000L,
                1424523725975002L, 1424523725987000L, 1424523725999003L,
                1424523726012000L, 1424523726038000L, 1424523726050000L));
        Set<Long> b = Sets.newTreeSet(Arrays.asList(1424523725341000L,
                1424523725491000L, 1424523725514000L, 1424523725534000L,
                1424523725584003L, 1424523725605000L, 1424523725641002L,
                1424523725659002L, 1424523725691000L, 1424523725724000L,
                1424523725753000L, 1424523725786002L, 1424523725801000L,
                1424523725823002L, 1424523725841000L, 1424523725937002L,
                1424523725963000L, 1424523725975002L, 1424523725987000L,
                1424523725999003L, 1424523726012000L, 1424523726050000L));
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Long> newHashSet()));
    }

    @Test
    public void testIntersection() {
        Set<Integer> a = Sets.newHashSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
    }

    @Test
    public void testMixedIntersection() {
        Set<Integer> a = Sets.newTreeSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
    }

    @Test
    public void testUnion() {
        Set<Integer> a = Sets.newHashSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.union(a, b),
                Sets.union(a, b).copyInto(Sets.<Integer> newHashSet()));
    }

    /**
     * Populate the set with some random integers.
     * 
     * @param set
     */
    private void populate(Set<Integer> set) {
        for (int i = 0; i < 100; i++) {
            if(Time.now() % 3 == 0) {
                set.add(i);
            }
        }
    }

}
