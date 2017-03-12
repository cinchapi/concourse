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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Unit tests that check the operational correctness of the Set API methods.
 * 
 * @author Jeff Nelson
 */
public class SetTest extends ConcourseIntegrationTest {

    @Test
    public void testSetInEmptyKey() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.set(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
    }
    
    @Test
    public void testSetInPopulatedKey(){
        int count = TestData.getScaleCount();
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        for(int i = 0; i < count; i++){
            client.add(key, count, record);
        }
        client.set(key, value, record);
        Assert.assertEquals(value, client.get(key, record));
        Assert.assertEquals(1, client.select(key, record).size());  
    }
    
    @Test
    public void testSetValueThatAlreadyExists(){
        Set<Object> values = Sets.newHashSet();
        String key = TestData.getSimpleString();
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
        Assert.assertEquals(1, client.select(key, record).size());
    }

}
