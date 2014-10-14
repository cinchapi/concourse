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
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.util.List;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link TArrays} class.
 * 
 * @author jnelson
 */
public class TArraysTest extends ConcourseBaseTest {

    @Test
    public void testConsistentHashing() {
        // Test that the byte buffer that is returned is consistent from JVM
        // session to JVM (e.g. it depends on persistent data).
        List<Integer> bytes = Lists.newArrayList(-58, 111, 114, 103, 46, 99,
                105, 110, 99, 104, 97, 112, 105, 46, 99, 111, 110, 99, 111,
                117, 114, 115, 101, 46, 115, 101, 114, 118, 101, 114, 46, 109,
                111, 100, 101, 108, 46, 84, 101, 120, 116, 19, 111, 114, 103,
                46, 99, 105, 110, 99, 104, 97, 112, 105, 46, 99, 111, 110, 99,
                111, 117, 114, 115, 101, 46, 115, 101, 114, 118, 101, 114, 46,
                109, 111, 100, 101, 108, 46, 86, 97, 108, 117, 101, 1, 111,
                114, 103, 46, 99, 105, 110, 99, 104, 97, 112, 105, 46, 99, 111,
                110, 99, 111, 117, 114, 115, 101, 46, 115, 101, 114, 118, 101,
                114, 46, 109, 111, 100, 101, 108, 46, 80, 114, 105, 109, 97,
                114, 121, 75, 101, 121);
        Object[] data = { Text.wrap("foo"),
                Value.wrap(Convert.javaToThrift("bar")), PrimaryKey.wrap(1) };
        ByteBuffer buf = TArrays.hash(data);
        for (int i = 0; i < buf.capacity(); i++) {
            Assert.assertEquals(buf.get(), bytes.get(i).byteValue());
        }
    }
    
    @Test
    public void testEqualObjectsHaveEqualHash(){
        Object[] data = {TestData.getText(), TestData.getValue(), TestData.getPrimaryKey()};
        ByteBuffer a = TArrays.hash(data);
        ByteBuffer b = TArrays.hash(data);
        Assert.assertEquals(a, b);
    }

}
