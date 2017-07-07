/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for the diff API methods.
 * 
 * @author Jeff Nelson
 */
public class DiffTest extends ConcourseIntegrationTest {
    
    @Test
    public void testDiffKeyRecordNoValuesAdded(){
        client.add("foo", 1, 1);
        client.add("foo", 2, 1);
        client.add("foo", 3, 1);
        Timestamp start = Timestamp.now();
        client.remove("foo", 2, 1);
        client.remove("foo", 1, 1);
        Map<Diff, Set<Object>> diff = client.diff("foo", 1, start);
        Assert.assertFalse(diff.containsKey(Diff.ADDED));
        Assert.assertEquals(Sets.newHashSet(2, 1), diff.get(Diff.REMOVED));
    }

    @Test
    public void testDiffKeyWithValueInIntersection() {
        long record1 = Variables.register("record1",
                client.add("name", "Jeff Nelson"));
        Timestamp start = Timestamp.now();
        long record2 = Variables.register("record2",
                client.add("name", "Jeff Nelson"));
        long record3 = Variables.register("record3",
                client.add("name", "Jeff Nelson"));
        client.clear(record1);
        Timestamp end = Timestamp.now();
        Map<Object, Map<Diff, Set<Long>>> diff = client
                .diff("name", start, end);
        Map<Diff, Set<Long>> inner = diff.get("Jeff Nelson");
        Set<Long> added = inner.get(Diff.ADDED);
        Set<Long> removed = inner.get(Diff.REMOVED);
        Assert.assertEquals(Sets.newHashSet(record2, record3), added);
        Assert.assertEquals(Sets.newHashSet(record1), removed);
    }
    
    @Test
    public void testDiffKeyWithEmptyIntersection(){
        String key = TestData.getSimpleString();
        client.add(key, 1, 1);
        Timestamp start = Timestamp.now();
        client.add(key, 2, 1);
        client.add(key, 1, 2);
        client.add(key, 3, 3);
        client.remove(key, 1, 2);
        Map<Object, Map<Diff, Set<Long>>> diff = client.diff(key, start);
        Assert.assertEquals(Sets.newHashSet(1L), diff.get(2).get(Diff.ADDED));
        Assert.assertEquals(Sets.newHashSet(3L), diff.get(3).get(Diff.ADDED));
        Assert.assertEquals(2, diff.size());
        Assert.assertEquals(1, diff.get(2).size());
        Assert.assertEquals(1, diff.get(3).size());
    }

}
