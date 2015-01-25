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
package org.cinchapi.concourse.server.concurrent;

import java.util.Map;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Unit tests for the {@link RangeTokenMap} data structure.
 * 
 * @author jnelson
 */
public class RangeTokenMapTest extends ConcourseBaseTest {

    private RangeTokenMap<String> map;

    @Override
    public void beforeEachTest() {
        map = RangeTokenMap.create();
    }

    @Test
    public void testPut() {
        Text key = TestData.getText();
        RangeToken token = getRangeToken(key);
        String value = TestData.getString();
        map.put(token, value);
        Assert.assertEquals(value, map.get(token));
    }

    @Test
    public void testRemove() {
        Text key = TestData.getText();
        RangeToken token = getRangeToken(key);
        String value = TestData.getString();
        map.put(token, value);
        map.remove(token);
        Assert.assertNull(map.get(token));
    }

    @Test
    public void testFilter() {
        Text key = TestData.getText();
        Map<RangeToken, String> expected = Maps.newHashMap();
        int count = 20;
        for (int i = 0; i < count; i++) {
            if(i % 2 == 0) {
                RangeToken token = getRangeToken(key);
                String value = TestData.getString();
                map.put(token, value);
                expected.put(token, value);
            }
            else {
                Text key2 = null;
                while (key2 == null || key2.equals(key)) {
                    key2 = TestData.getText();
                }
                map.put(getRangeToken(key2), TestData.getString());
            }
        }
        System.out.println(map.filter(key).size());
        System.out.println(expected.size());
        Assert.assertEquals(expected, map.filter(key));
    }

    // TODO: add more tests
    /**
     * Return a random {@link RangeToken} that has the specified {@code key}
     * component.
     * 
     * @param key
     * @return the RangeToken
     */
    private RangeToken getRangeToken(Text key) {
        if(Time.now() % 2 == 0) {
            return RangeToken.forReading(key, Operator.EQUALS,
                    TestData.getValue());
        }
        else {
            return RangeToken.forWriting(key, TestData.getValue());
        }
    }

}
