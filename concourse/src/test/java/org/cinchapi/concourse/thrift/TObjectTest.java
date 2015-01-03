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
package org.cinchapi.concourse.thrift;

import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the constraints promised by the {@link TObject} class.
 * 
 * @author jnelson
 */
public class TObjectTest {

    @Test
    public void testTagAndStringAreEqual() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag, string);
    }

    @Test
    public void testTagAndStringHaveSameHashCode() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag.hashCode(), string.hashCode());
    }

    @Test
    public void testTagAndStringDoNotMatch() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertFalse(tag.matches(string));
        Assert.assertFalse(string.matches(tag));
    }

}
