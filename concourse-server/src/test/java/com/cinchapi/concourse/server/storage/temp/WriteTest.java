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
package com.cinchapi.concourse.server.storage.temp;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Write}.
 * 
 * @author Jeff Nelson
 */
public class WriteTest extends ByteableTest {

    @Test
    public void testMatchesSameType() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertTrue(Write.add(key, value, record).matches(
                Write.add(key, value, record)));
        Assert.assertTrue(Write.remove(key, value, record).matches(
                Write.remove(key, value, record)));
        Assert.assertTrue(Write.notStorable(key, value, record).matches(
                Write.notStorable(key, value, record)));
    }

    @Test
    public void testMatchesDiffType() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertFalse(Write.add(key, value, record).matches(
                Write.remove(key, value, record)));
        Assert.assertFalse(Write.add(key, value, record).matches(
                Write.notStorable(key, value, record)));
        Assert.assertFalse(Write.remove(key, value, record).matches(
                Write.notStorable(key, value, record)));
    }

    @Test
    public void testEqualsDiffType() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertEquals(Write.add(key, value, record),
                Write.remove(key, value, record));
        Assert.assertEquals(Write.add(key, value, record),
                Write.notStorable(key, value, record));
        Assert.assertEquals(Write.remove(key, value, record),
                Write.notStorable(key, value, record));
    }

    @Test
    public void testEqualsSameType() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertEquals(Write.add(key, value, record),
                Write.add(key, value, record));
        Assert.assertEquals(Write.remove(key, value, record),
                Write.remove(key, value, record));
        Assert.assertEquals(Write.notStorable(key, value, record),
                Write.notStorable(key, value, record));
    }

    @Override
    protected Class<Write> getTestClass() {
        return Write.class;
    }

}
