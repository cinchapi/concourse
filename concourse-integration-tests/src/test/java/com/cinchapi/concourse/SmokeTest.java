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
package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;

/**
 * A collection of very quick and basic UAT tests to server as a sanity check
 * that things are functioning properly.
 * 
 * @author Jeff Nelson
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
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.add(key, value, record);
        Assert.assertFalse(client.add(key, value, record));
    }
    
    @Test(expected = RuntimeException.class)
    public void testCannotAddEmptyKey(){
        Assert.assertFalse(client.add("", "foo", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add(string, "foo", 1));
    }
    
    @Test(expected = RuntimeException.class)
    public void testCannotAddEmptyStringValue(){
        Assert.assertFalse(client.add("foo", "", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add("foo", string, 1));
    }
    
    @Test(expected = InvalidArgumentException.class)
    public void testCannotAddNullValue(){
        client.add("foo", null, 1);
    }
    
    @Test
    public void testCannotRemoveIfNotAdded(){
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        Assert.assertFalse(client.remove(key, value, record));
    }

}
