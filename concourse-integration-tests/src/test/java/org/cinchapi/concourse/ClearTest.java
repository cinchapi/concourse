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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Concourse#clear(long)} API method. The clear method
 * takes any record and removes all the keys and the values contained inside the
 * key.
 * 
 * @author jnelson
 */
public class ClearTest extends ConcourseIntegrationTest {

    @Test
    public void testBrowseEmptyRecord() {
    	client.clear(1);
        Assert.assertTrue(client.browse(1).isEmpty());
    }

    @Test
    public void testClear() {
        long record = TestData.getLong();
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.add("c", 1, record);
        client.add("c", 2, record);
        client.add("c", 3, record);
        client.add("d", 1, record);
        client.add("d", 2, record);
        client.add("d", 3, record);
        client.clear(record);
        Assert.assertTrue(client.browse(record).isEmpty());
    }
    
    @Test
    public void testClearRecordList() {
    	long record = 1;
    	long record2 = 2;
    	long record3 = 3;
    	List<Long> recordsList = new ArrayList<Long>();
    	recordsList.add(record);
    	recordsList.add(record2);
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.add("c", 1, record2);
        client.add("c", 2, record2);
        client.add("c", 3, record2);
        client.add("d", 1, record3);
        client.add("d", 2, record3);
        client.add("d", 3, record3);
        client.clear(recordsList);
        Assert.assertEquals(client.browse(record), client.browse(record2));
        Assert.assertFalse(client.browse(record3).isEmpty());
    }

}