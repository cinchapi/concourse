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
package com.cinchapi.concourse.server.plugin.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.data.TrackingLinkedHashMultimap;
import com.cinchapi.concourse.server.plugin.data.TrackingMultimap;
import com.cinchapi.concourse.server.plugin.data.TrackingMultimap.VariableType;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.Random;

import org.junit.Assert;

/**
 * Tests whether the {@link TrackingMultimap} calculates and returns the correct kind of {@link VariableType}.
 * This is useful for characterizing the data for the visualization engine.
 *
 */
public class TrackingMultimapVariableTypeTest extends ConcourseBaseTest {
    
    private TrackingMultimap<Object, Long> map;
    
    @Before
    public void beforeEachTest() {
        map = TrackingLinkedHashMultimap.create();
    }
    
    @After
    public void afterEachTest() {
        map = null;
    }
    
    @Test
    public void testDichotomous() {
        int count = new java.util.Random().nextInt(1) + 1;  // either a 1 or a 2
        for(int i = 0; i < count; i++) {
            map.insert(Random.getObject(), Random.getLong());
        }
        Assert.assertEquals(VariableType.DICHOTOMOUS, map.variableType());
    }
    
    @Test
    public void testNominal() {
        int count = new java.util.Random().nextInt(10) + 3;  // in range (3, 12)
        Variables.register("count", count);
        for(int i = 0; i < count; i++) {
            Object key = Variables.register("key_"+i, Random.getObject());
            Long value = Variables.register("value_"+i, Random.getLong());
            map.insert(key, value);
        }
        Assert.assertEquals(VariableType.NOMINAL, map.variableType());
    }
    
    @Test
    public void testInterval() {
        int count = new java.util.Random().nextInt(100000) + 13;  // in range (13, big number)
        for(int i = 0; i < count; i++) {
            map.insert(Random.getObject(), Random.getLong());
        }
        Assert.assertEquals(VariableType.INTERVAL, map.variableType());
    }

}
