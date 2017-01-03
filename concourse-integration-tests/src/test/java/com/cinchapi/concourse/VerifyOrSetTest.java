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

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for the verifyOrSet method in {@link Concourse}.
 * 
 * @author Jeff Nelson
 */
public class VerifyOrSetTest extends ConcourseIntegrationTest {

    @Test
    public void testVerifyOrSetExistingValue() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.add(key, value, record);
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.audit(key, record).size());
    }

    @Test
    public void testVerifyOrSetNewValue() {
        String key = TestData.getSimpleString();
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
        String key = Variables.register("key", TestData.getSimpleString());
        long record = Variables.register("record", TestData.getLong());
        int count = Variables.register("count", TestData.getScaleCount());
        Object value = null;
        for (int i = 0; i < count; i++) {
            Object obj = null;
            while (obj == null || client.verify(key, obj, record)) {
                obj = Variables.register("obj_" + i, TestData.getObject());
            }
            client.add(key, obj, record);
            if(i == 0 || TestData.getScaleCount() % 3 == 0) {
                value = obj;
            }
        }
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.select(key, record).size());
    }

    @Test
    public void testVerifyOrSetInEmptyRecord() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.verifyOrSet(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
    }

}
