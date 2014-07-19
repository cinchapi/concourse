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
package org.cinchapi.concourse;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the verifyOrSet method in {@link Concourse}.
 * 
 * @author jnelson
 */
public class VerifyOrSetTest extends ConcourseIntegrationTest {

    @Test
    public void testVerifyOrSetExistingValue() {
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.add(key, value, record);
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.audit(key, record).size());
    }

    @Test
    public void testVerifyOrSetNewValue() {
        String key = TestData.getString();
        Object value1 = TestData.getObject();
        Object value2 = null;
        while (value2 == null || value1.equals(value2)) {
            value2 = TestData.getObject();
        }
        long record = TestData.getLong();
        client.add(key, value1, record);
        client.verifyOrSet(key, value2, record);
        Assert.assertEquals(value2, client.get(key, record));
    }

    @Test
    public void testVerifyOrSetClearsOtherValues() {
        String key = TestData.getString();
        long record = TestData.getLong();
        int count = TestData.getScaleCount();
        Object value = null;
        for (int i = 0; i < count; i++) {
            Object obj = null;
            while (obj == null || client.verify(key, obj, record)) {
                obj = TestData.getObject();
            }
            client.add(key, obj, record);
            if(i == 0 || TestData.getScaleCount() % 3 == 0) {
                value = obj;
            }
        }
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.fetch(key, record).size());
    }
    
    @Test
    public void testVerifyOrSetInEmptyRecord(){
        String key = TestData.getString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
    }

}
