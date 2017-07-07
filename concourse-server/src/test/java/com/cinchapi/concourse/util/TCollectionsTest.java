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

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.TCollections;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link TCollections} until class.
 * 
 * @author Jeff Nelson
 */
public class TCollectionsTest {

    @Test
    public void testLargerBetween() {
        int aSize = TestData.getScaleCount();
        int bSize = (int) (aSize * 1.35);
        Collection<Integer> a = getCollection(aSize);
        Collection<Integer> b = getCollection(bSize);
        Assert.assertEquals(b, TCollections.largerBetween(a, b));
    }
    
    @Test
    public void testLargerBetweenSameSize(){
        int aSize = TestData.getScaleCount();
        Collection<Integer> a = getCollection(aSize);
        Collection<Integer> b = getCollection(aSize);
        Assert.assertEquals(b, TCollections.largerBetween(a, b)); 
    }

    @Test
    public void testSmallerBetween() {
        int aSize = TestData.getScaleCount();
        int bSize = (int) (aSize * 1.35);
        Collection<Integer> a = getCollection(aSize);
        Collection<Integer> b = getCollection(bSize);
        Assert.assertEquals(a, TCollections.smallerBetween(a, b));
    }
    
    @Test
    public void testSmallerBetweenSameSize(){
        int aSize = TestData.getScaleCount();
        Collection<Integer> a = getCollection(aSize);
        Collection<Integer> b = getCollection(aSize);
        Assert.assertEquals(a, TCollections.smallerBetween(a, b)); 
    }

    /**
     * Return a collection with {@code size} elements.
     * 
     * @param size
     * @return the collection
     */
    private Collection<Integer> getCollection(int size) {
        Collection<Integer> collection;
        int num = TestData.getInt();
        if(num % 3 == 0) {
            collection = Sets.newTreeSet();
        }
        else if(num % 2 == 0) {
            collection = Lists.newArrayList();
        }
        else {
            collection = Sets.newHashSet();
        }
        for (int i = 0; i < size; i++) {
            collection.add(i);
        }
        return collection;
    }

}
