/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.ConcourseBaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LongBitSet}.
 * 
 * @author jnelson
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


    /**
     * Return a random position.
     * 
     * @return the position
     */
    private static int getPosition() {
        return TestData.getInt();
    }

}
