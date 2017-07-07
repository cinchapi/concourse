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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for proportion retrieval of keys and values within the
 * {@link TrackingMultimap}.
 * Makes sure that proportions and uniqueness coefficient are calculated
 * correctly.
 *
 */
public class TrackingMultimapProportionsTest extends TrackingMultimapBaseTest<String, Integer> {

    @Test
    public void testProportion() {
        double count1 = Random.getScaleCount();
        double count2 = Random.getScaleCount();
        String key1 = Random.getSimpleString();
        String key2 = Random.getSimpleString();
        Set<Integer> value1 = new HashSet<Integer>();
        Set<Integer> value2 = new HashSet<Integer>();
        for (int i = 0; i < count1; i++) {
            value1.add(Random.getInt());
        }
        for (int i = 0; i < count2; i++) {
            value2.add(Random.getInt());
        }
        map.put(key1, value1);
        map.put(key2, value2);
        double total = count1 + count2;
        Assert.assertEquals(count1 / total, map.proportion(key1), 0);
        Assert.assertEquals(count2 / total, map.proportion(key2), 0);
    }

    @Test
    public void testUniqueness() {
        double sampleCount = Random.getScaleCount();
        List<Double> counts = new ArrayList<Double>();
        for (int i = 0; i < sampleCount; i++) {
            double count = Random.getScaleCount();
            counts.add(count);
            String key = Random.getSimpleString();
            Set<Integer> value = new HashSet<Integer>();
            for (int j = 0; j < count; j++) {
                value.add(Random.getInt());
            }
            map.put(key, value);
        }
        double total = 0;
        for (Double count : counts) {
            total += count;
        }
        double uniqueness = 0;
        for (Double count : counts) {
            uniqueness += Math.pow(count / total, 2);
        }
        uniqueness = 1 - Math.sqrt(uniqueness);
        Assert.assertEquals(uniqueness, map.uniqueness(), 0);
    }

    @Test
    public void testDistinctiveness() {
        double sampleCount = Random.getScaleCount();
        List<Double> counts = new ArrayList<Double>();
        for (int i = 0; i < sampleCount; i++) {
            double count = Random.getScaleCount();
            counts.add(count);
            String key = Random.getSimpleString();
            Set<Integer> value = new HashSet<Integer>();
            for (int j = 0; j < count; j++) {
                value.add(Random.getInt());
            }
            map.put(key, value);
        }
        double total = 0;
        for (Double count : counts) {
            total += count;
        }
        Assert.assertEquals(sampleCount / total, map.distinctiveness(), 0);
    }

}
