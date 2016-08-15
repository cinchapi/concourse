/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>
 * Tests to confirm that data is entered correctly into the {@link Dataset}, and
 * retrieved via inversion.
 * </p>
 * 
 * @author Aditya Srinivasan
 */
public class DatasetTest extends ConcourseBaseTest {

    private Dataset<Long, String, Object> dataset;

    @Override
    public void beforeEachTest() {
        dataset = new ResultDataset();
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
            Object subkey = Random.getObject();
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
        Map<String, Map<Object, Set<Long>>> inverted = new HashMap<String, Map<Object, Set<Long>>>();
        Map<Long, Map<String, Set<Object>>> original = new HashMap<Long, Map<String, Set<Object>>>();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            String string = Random.getString();
            Long loong = Random.getLong();
            Object object = Random.getObject();

            // EXPECTATION OF INVERTED
            Map<Object, Set<Long>> invertedSubmap = inverted.get(string);
            if(invertedSubmap == null) {
                invertedSubmap = new HashMap<Object, Set<Long>>();
            }

            Set<Long> invertedSubset = invertedSubmap.get(object);
            if(invertedSubset == null) {
                invertedSubset = new HashSet<Long>();
            }

            invertedSubset.add(loong);
            invertedSubmap.put(object, invertedSubset);
            inverted.put(string, invertedSubmap);

            // EXPECTATION OF ORIGINAL
            Map<String, Set<Object>> originalSubmap = original.get(loong);
            if(originalSubmap == null) {
                originalSubmap = new HashMap<String, Set<Object>>();
            }

            Set<Object> originalSubset = originalSubmap.get(string);
            if(originalSubset == null) {
                originalSubset = new HashSet<Object>();
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
    public void testInsertEntry() {
        Map<String, Map<Object, Set<Long>>> expected = new HashMap<String, Map<Object, Set<Long>>>();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            String key = Random.getString();
            Map<Object, Set<Long>> value = expected.get(key);
            if(value == null) {
                value = new HashMap<Object, Set<Long>>();
            }
            Object subkey = Random.getObject();
            Set<Long> subvalue = value.get(subkey);
            if(subvalue == null) {
                subvalue = new HashSet<Long>();
            }
            Long element = Random.getLong();
            subvalue.add(element);
            value.put(subkey, subvalue);
            expected.put(key, value);
            DatasetEntry<Long, String, Object> entry = new DatasetEntry<Long, String, Object>(
                    element, key, subkey);
            dataset.insert(entry);
        }
        Assert.assertEquals(expected, dataset.invert());
    }

    @Test
    public void testDuplicateInsertEntry() {
        String key1 = "key1";
        Long record1 = 0L;
        Boolean value1 = true;

        String key2 = "key2";
        Long record2 = 1L;
        Boolean value2 = false;

        DatasetEntry<Long, String, Object> entry1 = new DatasetEntry<Long, String, Object>(
                record1, key1, value1);
        DatasetEntry<Long, String, Object> duplicate1 = new DatasetEntry<Long, String, Object>(
                record1, key1, value1);

        DatasetEntry<Long, String, Object> entry2 = new DatasetEntry<Long, String, Object>(
                record2, key2, value2);
        DatasetEntry<Long, String, Object> duplicate2 = new DatasetEntry<Long, String, Object>(
                record2, key2, value2);

        dataset.insert(entry1);
        dataset.insert(duplicate1);
        dataset.insert(entry2);
        dataset.insert(duplicate2);

        Dataset<Long, String, Object> expected = new ResultDataset();

        expected.insert(0L, "key1", true); // entry1
        expected.insert(1L, "key2", false); // entry2

        Assert.assertEquals(expected.invert(), dataset.invert());
    }

    @Test
    public void testInsertSetEntry() {
        Map<String, Map<Object, Set<Long>>> expected = new HashMap<String, Map<Object, Set<Long>>>();
        int outerCount = Random.getScaleCount();
        int innerCount = Random.getScaleCount();
        for (int i = 0; i < outerCount; i++) {
            Set<DatasetEntry<Long, String, Object>> entries = Sets.newHashSet();
            for (int j = 0; j < innerCount; j++) {
                String key = Random.getString();
                Map<Object, Set<Long>> value = expected.get(key);
                if(value == null) {
                    value = new HashMap<Object, Set<Long>>();
                }
                Object subkey = Random.getObject();
                Set<Long> subvalue = value.get(subkey);
                if(subvalue == null) {
                    subvalue = new HashSet<Long>();
                }
                Long element = Random.getLong();
                subvalue.add(element);
                value.put(subkey, subvalue);
                expected.put(key, value);
                DatasetEntry<Long, String, Object> entry = new DatasetEntry<Long, String, Object>(
                        element, key, subkey);
                entries.add(entry);
            }
            dataset.insert(entries);
        }

        Assert.assertEquals(expected, dataset.invert());
    }

    @Test
    public void testDuplicateInsertSetEntry() {
        dataset.insert(0L, "occupation", "software engineer");
        dataset.insert(4L, "alive", true);

        Set<DatasetEntry<Long, String, Object>> entries = Sets.newHashSet();

        entries.add(new DatasetEntry<>(0L, "occupation", "software engineer")); // duplicate
        entries.add(new DatasetEntry<>(1L, "first_name", "Foo"));
        entries.add(new DatasetEntry<>(2L, "last_name", "Bar"));
        entries.add(new DatasetEntry<>(3L, "email", "foo@bar.com"));
        entries.add(new DatasetEntry<>(4L, "alive", true)); // duplicate
        entries.add(new DatasetEntry<>(5L, "age", 27));
        entries.add(new DatasetEntry<>(6L, "education", "bachelor's"));
        entries.add(new DatasetEntry<>(7L, "has_pets", false));

        Assert.assertEquals(6, dataset.insert(entries));
    }

    @Test
    public void testDuplicateInsertSetEntryRandom() {
        List<DatasetEntry<Long, String, Object>> possibleEntries = Lists
                .newArrayList();
        int count = Random.getScaleCount();

        for (int i = 0; i < count; i++) {
            possibleEntries.add(new DatasetEntry<>(Random.getLong(),
                    Random.getSimpleString(), Random.getObject()));
        }

        int subcount = (int) (count / 5);

        java.util.Random r = new java.util.Random();

        Set<DatasetEntry<Long, String, Object>> preinsertedEntries = Sets
                .newHashSet();

        for (int i = 0; i < subcount; i++) {
            DatasetEntry<Long, String, Object> entry = possibleEntries
                    .get(r.nextInt(count));
            preinsertedEntries.add(entry);
            dataset.insert(entry);
        }

        Set<DatasetEntry<Long, String, Object>> postinsertedEntries = Sets
                .newHashSet();

        for (int i = 0; i < subcount; i++) {
            DatasetEntry<Long, String, Object> entry = possibleEntries
                    .get(r.nextInt(count));
            postinsertedEntries.add(entry);
        }

        Set<DatasetEntry<Long, String, Object>> commonEntries = Sets
                .newHashSet(preinsertedEntries);
        commonEntries.retainAll(postinsertedEntries);

        int numberUncommon = postinsertedEntries.size() - commonEntries.size();

        Assert.assertEquals(numberUncommon,
                dataset.insert(postinsertedEntries));

    }

}
