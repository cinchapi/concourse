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

import java.util.Set;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Unit tests that check the operational correctness of the Set API methods.
 * 
 * @author jnelson
 */
public class SetTest extends ConcourseIntegrationTest {

    @Test
    public void testSetInEmptyKey() {
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.set(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
    }
    
    @Test
    public void testSetInPopulatedKey(){
        int count = TestData.getScaleCount();
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        for(int i = 0; i < count; i++){
            client.add(key, count, record);
        }
        client.set(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.fetch(key, record).size());  
    }
    
    @Test
    public void testSetValueThatAlreadyExists(){
        Set<Object> values = Sets.newHashSet();
        String key = TestData.getString();
        long record = TestData.getLong();
        for(int i = 0; i < TestData.getScaleCount(); i++){
            values.add(TestData.getObject());
        }
        Object value = Iterables.getFirst(values, TestData.getObject());
        for(Object v : values){
            value = TestData.getInt() % 4 == 0 ? v : value;
            client.add(key, v, record);
        }
        Assert.assertTrue(client.verify(key, value, record));
        client.set(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.fetch(key, record).size());
    }

}
