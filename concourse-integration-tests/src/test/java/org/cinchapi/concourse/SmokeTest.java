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
package org.cinchapi.concourse;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * A collection of very quick and basic UAT tests to server as a sanity check
 * that things are functioning properly.
 * 
 * @author jnelson
 */
public class SmokeTest extends ConcourseIntegrationTest {
    
    @Test
    public void testAddBoolean(){
        Assert.assertTrue(client.add("foo", TestData.getBoolean(), 1));
    }
    
    @Test
    public void testAddDouble(){
        Assert.assertTrue(client.add("foo", TestData.getDouble(), 1));
    }
    
    @Test
    public void testAddFloat(){
        Assert.assertTrue(client.add("foo", TestData.getFloat(), 1));
    }

    @Test
    public void testAddInteger() {
        Assert.assertTrue(client.add("foo", TestData.getInt(), 1));
    }

    @Test
    public void testAddLong() {
        Assert.assertTrue(client.add("foo", TestData.getLong(), 1));
    }
    
    @Test
    public void testAddString(){
        Assert.assertTrue(client.add("foo", TestData.getString(), 1));
    }
    
    @Test
    public void testCannotAddDuplicate(){
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.add(key, value, record);
        Assert.assertFalse(client.add(key, value, record));
    }
    
    @Test
    public void testCannotAddEmptyKey(){
        Assert.assertFalse(client.add("", "foo", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add(string, "foo", 1));
    }
    
    @Test
    public void testCannotAddEmptyStringValue(){
        Assert.assertFalse(client.add("foo", "", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add("foo", string, 1));
    }
    
    @Test(expected = NullPointerException.class)
    public void testCannotAddNullValue(){
        client.add("foo", null, 1);
    }
    
    @Test
    public void testCannotRemoveIfNotAdded(){
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        Assert.assertFalse(client.remove(key, value, record));
    }

}
