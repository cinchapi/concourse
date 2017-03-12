/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.Arrays;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TSets;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link TSets} utility class.
 * 
 * @author Jeff Nelson
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
