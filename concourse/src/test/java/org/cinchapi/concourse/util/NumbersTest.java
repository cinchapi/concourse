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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Numbers}.
 * 
 * @author jnelson
 */
public class NumbersTest {

    @Test
    public void testMax() {
        Assert.assertEquals(10, Numbers.max(1, 8, 2.4, 3.789, 10, -11));
    }
    
    @Test
    public void testMin() {
        Assert.assertEquals(-11, Numbers.min(1, 8, 2.4, 3.789, 10, -11));
    }
    
    @Test
    public void testCompareNumbers(){
       Number a = Random.getNegativeNumber();
       Number b = Random.getPositiveNumber();
       Assert.assertTrue(Numbers.compare(a, b) < 0);
    }

}
