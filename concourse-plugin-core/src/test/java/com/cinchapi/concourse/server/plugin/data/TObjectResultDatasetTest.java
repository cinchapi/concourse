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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.data.Dataset;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;

/**
 * <p>
 * Tests to confirm that data is entered correctly into the {@link Dataset}, and
 * retrieved via inversion.
 * </p>
 * 
 * @author Aditya Srinivasan
 */
public class TObjectResultDatasetTest extends ConcourseBaseTest {

    private Dataset<Long, String, TObject> dataset;

    @Override
    public void beforeEachTest() {
        dataset = new TObjectResultDataset();
    }

    @Override
    public void afterEachTest() {
        dataset = null;
    }

    @Test
    public void testInsert() {
        Map<String, Map<Object, Set<Long>>> expected = new HashMap<String, Map<Object, Set<Long>>>();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            String key = Random.getString();
            Map<Object, Set<Long>> value = expected.get(key);
            if(value == null) {
                value = new HashMap<Object, Set<Long>>();
            }
            TObject subkey = Convert.javaToThrift(Random.getObject());
            Set<Long> subvalue = value.get(subkey);
            if(subvalue == null) {
                subvalue = new HashSet<Long>();
            }
            Long element = Random.getLong();
            subvalue.add(element);
            value.put(subkey, subvalue);
            expected.put(key, value);
            dataset.insert(element, key, subkey);
        }
        Assert.assertEquals(expected, dataset.invert());
    }

    @Test
    public void testPut() {
        Map<String, Map<TObject, Set<Long>>> inverted = new HashMap<String, Map<TObject, Set<Long>>>();
        Map<Long, Map<String, Set<TObject>>> original = new HashMap<Long, Map<String, Set<TObject>>>();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            String string = Random.getString();
            Long loong = Random.getLong();
            TObject object = Convert.javaToThrift(Random.getObject());

            // EXPECTATION OF INVERTED
            Map<TObject, Set<Long>> invertedSubmap = inverted.get(string);
            if(invertedSubmap == null) {
                invertedSubmap = new HashMap<TObject, Set<Long>>();
            }

            Set<Long> invertedSubset = invertedSubmap.get(object);
            if(invertedSubset == null) {
                invertedSubset = new HashSet<Long>();
            }

            invertedSubset.add(loong);
            invertedSubmap.put(object, invertedSubset);
            inverted.put(string, invertedSubmap);

            // EXPECTATION OF ORIGINAL
            Map<String, Set<TObject>> originalSubmap = original.get(loong);
            if(originalSubmap == null) {
                originalSubmap = new HashMap<String, Set<TObject>>();
            }

            Set<TObject> originalSubset = originalSubmap.get(string);
            if(originalSubset == null) {
                originalSubset = new HashSet<TObject>();
            }

            originalSubset.add(object);
            originalSubmap.put(string, originalSubset);
            original.put(loong, originalSubmap);

            // PUT INTO DATASET
            dataset.put(loong, originalSubmap);
        }
        Assert.assertEquals(inverted, dataset.invert());
    }
    
    @Test
    public void testGetRow(){
        dataset.insert(1L, "key", Convert.javaToThrift(Random.getObject()));
        Assert.assertNotNull(dataset.get(1L));
    }

}
