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
    public void testIntersection(){
        Set<Integer> a = Sets.newHashSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
    }
    
    @Test
    public void testMixedIntersection(){
        Set<Integer> a = Sets.newTreeSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.intersection(a, b), Sets.intersection(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
    }
    
    @Test
    public void testUnion(){
        Set<Integer> a = Sets.newHashSet();
        Set<Integer> b = Sets.newHashSet();
        populate(a);
        populate(b);
        Assert.assertEquals(TSets.union(a, b), Sets.union(a, b)
                .copyInto(Sets.<Integer> newHashSet()));
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
