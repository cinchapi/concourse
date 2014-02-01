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
package org.cinchapi.concourse;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.StandardActions;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A collection of very basic UAT tests to server as a sanity check that things
 * are functioning properly.
 * 
 * @author jnelson
 */
public class SmokeTest extends ConcourseIntegrationTest {

    @Test
    public void test1() {
        StandardActions.importWordsDotText(client);
        StandardActions.wait(75, TimeUnit.SECONDS);
        StandardActions.import1000Longs(client);
        StandardActions.wait(10, TimeUnit.SECONDS);
        Set<Long> records = client.find("count",
                Operator.GREATER_THAN_OR_EQUALS, 0);
        Assert.assertEquals(1000, records.size());
        int expected = 0;
        for (long record : records) {
            Assert.assertEquals(expected, record);
            expected++;
        }
    }
    
    @Test
    public void test2(){
        StandardActions.importWordsDotText(client);
        StandardActions.wait(75, TimeUnit.SECONDS);
        Set<Long> a = client.search("strings", "aa");
        Set<Long> b = client.search("strings", "a aa");
        Set<Long> c = client.search("strings", "foo aa");
        Assert.assertFalse(a.isEmpty());
        Assert.assertEquals(a, b);
        Assert.assertTrue(c.isEmpty());
    }
    
    @Test(expected = NullPointerException.class)
    public void testCannotAddNullValue(){
        client.add("foo", null, 1);
    }
    
    @Test
    @Ignore("CON-21")
    public void testCannotAddEmptyStringValue(){
        Assert.assertFalse(client.add("foo", "", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add("foo", string, 1));
    }
    
    @Test
    @Ignore("CON-21")
    public void testCannotAddEmptyKey(){
        Assert.assertFalse(client.add("", "foo", 1));
        String string = "";
        for(int i = 0; i < TestData.getScaleCount(); i++){
            string+= " ";
        }
        Assert.assertFalse(client.add(string, "foo", 1));
    }

}
