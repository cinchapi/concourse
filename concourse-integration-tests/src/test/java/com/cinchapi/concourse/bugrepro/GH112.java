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
package com.cinchapi.concourse.bugrepro;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Unit test to repro issue GH-112
 * 
 * @author Jeff Nelson
 */
public class GH112 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        Map<String, Object> data = Maps.newHashMap();
        data.put("title", "Director of Engineering");
        data.put("name", "Jane Doe");
        data.put("salary", 20.0);
        data.put("role", "Director");
        long record = client.insert(data);
        List<String> keys = Lists.newArrayList("name", "salary", "title");
        Map<Long, Map<String, Set<Object>>> results = client.select(keys,
                "role = Director and salary > 10");
        Iterator<String> it = results.get(record).keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            Assert.assertEquals(key, keys.remove(0));
        }
    }

}
