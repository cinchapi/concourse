/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.model;

import org.cinchapi.concourse.server.io.ByteableTest;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Text}.
 * 
 * @author jnelson
 */
public class TextTest extends ByteableTest {

    @Test
    public void testCompareTo() {
        String s1 = TestData.getString();
        String s2 = TestData.getString();
        Text t1 = Text.wrap(s1);
        Text t2 = Text.wrap(s2);
        Assert.assertEquals(s1.compareTo(s2), t1.compareTo(t2));
    }

    @Override
    protected Class<Text> getTestClass() {
        return Text.class;
    }
    
    @Test
    public void testDeserializationWithTrailingWhitespace(){
        String s = "Youtube Embed Link ";
        Text t1 = Text.wrap(s);
        Text t2 = Text.fromByteBuffer(t1.getBytes());
        Assert.assertEquals(t1, t2);
    }

}
