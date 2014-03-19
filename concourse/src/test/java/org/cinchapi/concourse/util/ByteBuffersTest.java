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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Unit tests for {@link ByteBuffers} utility methods
 * 
 * @author jnelson
 */
public class ByteBuffersTest {
    
    @Test
    public void testDeserializeStringTrailingWhitespace(){
        String string = Random.getString().trim();
        int numWhiteSpaces = Random.getScaleCount();
        string = Strings.padEnd(string, string.length() + numWhiteSpaces, ' ');
        ByteBuffer bytes = ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }
    
    @Test
    public void testDeserializeStringLeadingWhitespace(){
        String string = Random.getString().trim();
        int numWhiteSpaces = Random.getScaleCount();
        string = Strings.padStart(string, string.length() + numWhiteSpaces, ' ');
        ByteBuffer bytes = ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }
    
    @Test
    public void testDeserializeStringLeadingAndTrailingWhitespace(){
        String string = Random.getString().trim();
        int numWhiteSpaces1 = Random.getScaleCount();
        int numWhiteSpaces2 = Random.getScaleCount();
        string = Strings.padStart(string, string.length() + numWhiteSpaces1, ' ');
        string = Strings.padEnd(string, string.length() + numWhiteSpaces2, ' ');
        ByteBuffer bytes = ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }
    
    @Test
    public void testDeserializeStringNoWhitespace(){
        String string = Random.getString().trim();
        ByteBuffer bytes = ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }

}
