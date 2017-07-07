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

import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.LongBitSet;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link LongBitSet}.
 * 
 * @author Jeff Nelson
 */
public class LongBitSetTest extends ConcourseBaseTest {

    private LongBitSet bitSet;

    @Override
    public void beforeEachTest() {
        bitSet = LongBitSet.create();
    }

    @Test
    public void testSetTrue() {
        int position = getPosition();
        bitSet.set(position, true);
        Assert.assertEquals(true, bitSet.get(position));
    }

    @Test
    public void testDefaultIsFalse() {
        int position = getPosition();
        Assert.assertEquals(false, bitSet.get(position));
    }

    @Test
    public void testSetTrueThenFalse() {
        int position = getPosition();
        bitSet.set(position, true);
        bitSet.set(position, false);
        Assert.assertEquals(false, bitSet.get(position));
    }
    
    @Test
    public void testIterator(){
        int count = TestData.getScaleCount() * 5;
        for(int i = 0; i < count; ++i){
            bitSet.set(TestData.getLong());
        }
        Iterator<Long> it = bitSet.iterator();
        Set<Long> actual = Sets.newLinkedHashSetWithExpectedSize(count);
        while(it.hasNext()){
            long next = it.next();
            actual.add(next);
        }
        Set<Long> expected = (Set<Long>) bitSet.toIterable();
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testGetAll(){
        int count = TestData.getScaleCount() * 6;
        Set<Long> expected = Sets.newLinkedHashSetWithExpectedSize(count);
        for(int i = 0; i < count; ++i){
            long value = TestData.getLong();
            bitSet.set(value);
            expected.add(value);
        }
        Assert.assertEquals(expected, bitSet.toIterable());
    }


    /**
     * Return a random position.
     * 
     * @return the position
     */
    private static int getPosition() {
        return TestData.getInt();
    }

}
