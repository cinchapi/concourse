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

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link TCollections} until class.
 * 
 * @author jnelson
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
