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
import org.cinchapi.concourse.server.concurrent.RangeTokenMap.JoinBy;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
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
    public void testGetReproA() {
        Text key = Text
                .wrap("ob1n03nfibohnt nnseh5 y pdf5xq9maz054e7oz q k0d wc wc7qly17waqq8icla64iugdx4 j x0ovg  w7aow9 wtk8qy");
        Value value = Value.wrap(Convert.javaToThrift(true));
        RangeToken equals = RangeToken.forReading(key, Operator.EQUALS, value);
        RangeToken write = RangeToken.forWriting(key, value);
        map.put(equals, "a");
        map.put(equals, "b");
        map.put(equals, "c");
        map.put(write, "d");
        map.put(equals, "e");
        map.put(equals, "f");
        map.put(equals, "g");
        map.put(equals, "h");
        map.put(equals, "i");
        map.put(equals, "j");
        Assert.assertEquals("j", map.get(equals));       
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
    public void testReadEqualsAndWriteCanCoExist() {
        Text key = Text.wrap("foo");
        Value value = Value.wrap(Convert.javaToThrift(false));
        RangeToken read = RangeToken.forReading(key, Operator.EQUALS, value);
        RangeToken write = RangeToken.forWriting(key, value);
        map.put(read, "read");
        map.put(write, "write");
        Assert.assertEquals(map.get(read), "read");
        Assert.assertEquals(map.get(write), "write");
    }

    @Test
    public void testOverwrite() {
        Text key = TestData.getText();
        RangeToken token = getRangeToken(key);
        String value = TestData.getString();
        String other = null;
        while (other == null || value.equals(other)) {
            other = TestData.getString();
        }
        map.put(token, value);
        map.put(token, other);
        Assert.assertEquals(other, map.get(token));
    }

    @Test
    public void testFilter() {
        Text key = TestData.getText();
        Map<RangeToken, String> expected = Maps.newHashMap();
        int count = 5;
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
        Assert.assertEquals(expected, map.filter(key));
    }

    /**
     * Populate the map with data in preparation for tests that verify the
     * {@link RangeTokenMap#filter(Text, Operator, Value, org.cinchapi.concourse.server.concurrent.RangeTokenMap.JoinBy, Operator, Value)
     * filter} functionality.
     * 
     * @return the key
     */
    private Text setupFilterTests() {
        Text key = TestData.getText();
        map.put(RangeToken.forReading(key, Operator.BETWEEN, v(1), v(10)), "A");
        map.put(RangeToken.forReading(key, Operator.GREATER_THAN, v(1)), "B");
        map.put(RangeToken.forReading(key, Operator.LESS_THAN, v(10)), "C");
        map.put(RangeToken.forReading(key, Operator.BETWEEN, v(3), v(6)), "D");
        map.put(RangeToken.forReading(key, Operator.GREATER_THAN, v(5)), "E");
        map.put(RangeToken.forReading(key, Operator.GREATER_THAN_OR_EQUALS,
                v(5)), "F");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(1)), "G");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(2)), "H");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(3)), "I");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(4)), "J");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(5)), "K");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(6)), "L");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(7)), "M");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(8)), "N");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(9)), "O");
        map.put(RangeToken.forReading(key, Operator.EQUALS, v(10)), "P");
        map.put(RangeToken.forWriting(key, v(1)), "Q");
        map.put(RangeToken.forWriting(key, v(2)), "R");
        map.put(RangeToken.forWriting(key, v(3)), "S");
        map.put(RangeToken.forWriting(key, v(4)), "T");
        map.put(RangeToken.forWriting(key, v(5)), "U");
        map.put(RangeToken.forWriting(key, v(6)), "V");
        map.put(RangeToken.forWriting(key, v(7)), "W");
        map.put(RangeToken.forWriting(key, v(8)), "X");
        map.put(RangeToken.forWriting(key, v(9)), "Y");
        map.put(RangeToken.forWriting(key, v(10)), "Z");
        map.put(RangeToken.forReading(key, Operator.NOT_EQUALS, v(1)), "AA");
        map.put(RangeToken.forReading(key, Operator.NOT_EQUALS, v(3)), "BB");
        map.put(RangeToken.forReading(key, Operator.NOT_EQUALS, v(5)), "CC");
        map.put(RangeToken.forReading(key, Operator.NOT_EQUALS, v(7)), "DD");
        map.put(RangeToken.forReading(key, Operator.NOT_EQUALS, v(9)), "EE");
        map.put(RangeToken.forReading(key, Operator.BETWEEN, v(1), v(3)), "FF");
        map.put(RangeToken.forReading(key, Operator.BETWEEN, v(9), v(10)), "GG");
        map.put(RangeToken.forReading(key, Operator.GREATER_THAN_OR_EQUALS,
                v(10)), "HH");
        map.put(RangeToken.forReading(key, Operator.BETWEEN, v(3), v(8)), "II");

        // fill with garbage
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            Text key0 = null;
            while (key0 == null || key.equals(key0)) {
                key0 = TestData.getText();
            }
            map.put(getRangeToken(key0), TestData.getString());
            key0 = null;
        }
        return key;
    }

    /**
     * Create a {@link Value} from the integer.
     * 
     * @param i
     * @return the corresponding Value
     */
    private Value v(int i) {
        return Value.wrap(Convert.javaToThrift(i));
    }

    @Test
    public void testFilterEq() {
        Text key = setupFilterTests();
        Map<RangeToken, String> filtered = map.filter(key, Operator.EQUALS,
                v(1), JoinBy.AND, Operator.EQUALS, v(1));
        Assert.assertEquals(2, filtered.size());
    }

    @Test
    public void testFilterEqReproA() {
        Text key = Text.wrap("caaa");
        Value value = Value.wrap(Convert.javaToThrift(1));
        map.put(RangeToken.forReading(key, Operator.EQUALS, value), "G");
        map.put(RangeToken.forReading(key, Operator.EQUALS, value), "H");
        map.put(RangeToken.forWriting(key, value), "Q");
        Map<RangeToken, String> filtered = map.filter(key, Operator.EQUALS,
                v(1), JoinBy.AND, Operator.EQUALS, v(1));
        Assert.assertEquals(2, filtered.size());
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
        return getRangeToken(key, TestData.getValue());
    }

    /**
     * Return a {@link RangeToken} that has the specified {@code key} and
     * {@code value} components, but a random operator.
     * 
     * @param key
     * @param value
     * @return the RangeToken
     */
    private RangeToken getRangeToken(Text key, Value value) {
        if(Time.now() % 2 == 0) {
            return RangeToken.forReading(key, Operator.EQUALS, value);
        }
        else {
            return RangeToken.forWriting(key, value);
        }
    }

}
