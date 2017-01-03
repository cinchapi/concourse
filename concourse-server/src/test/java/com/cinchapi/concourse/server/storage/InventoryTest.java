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
package com.cinchapi.concourse.server.storage;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.Inventory;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link Inventory} class.
 * 
 * @author Jeff Nelson
 */
public class InventoryTest extends ConcourseBaseTest {

    private String backingStore;
    private Inventory inventory;

    @Override
    public void beforeEachTest() {
        backingStore = TestData.getTemporaryTestFile();
        inventory = Inventory.create(backingStore);
    }

    @Override
    public void afterEachTest() {
        FileSystem.deleteFile(backingStore);
    }

    @Test
    public void testAdd() {
        int count = TestData.getScaleCount() * TestData.getScaleCount();
        Set<Long> longs = Sets.newHashSet();
        while (longs.size() < count) {
            longs.add(TestData.getLong());
        }
        for (long l : longs) {
            inventory.add(l);
            Assert.assertTrue(inventory.contains(l));
        }
    }

    @Test
    public void testContains() {
        int count = TestData.getScaleCount() * TestData.getScaleCount();
        Set<Long> longs = Sets.newHashSet();
        while (longs.size() < count) {
            longs.add(TestData.getLong());
        }
        for (long l : longs) {
            Assert.assertFalse(inventory.contains(l));
            inventory.add(l);
            Assert.assertTrue(inventory.contains(l));
        }
    }

    @Test
    public void testContainsAfterDoubleAdd() {
        long l = TestData.getLong();
        inventory.add(l);
        inventory.add(l);
        Assert.assertTrue(inventory.contains(l));
    }

    @Test
    public void testNotDirtyIfExistingLongIsAdded() {
        long l = TestData.getLong();
        inventory.add(l);
        inventory.sync();
        inventory.add(l);
        Assert.assertTrue(inventory.dirty.isEmpty());
    }

    @Test
    public void testDeserialization() {
        int count = TestData.getScaleCount();
        Set<Long> longs = Sets.newHashSet();
        while (longs.size() < count) {
            longs.add(TestData.getLong());
        }
        for (long l : longs) {
            inventory.add(l);
        }
        inventory.sync();
        inventory = Inventory.create(backingStore);
        for (long l : longs) {
            Assert.assertTrue(inventory.contains(l));
        }
    }

    @Test
    public void testDeserializationAfterIncrementalSyncs() {
        int count = TestData.getScaleCount();
        Set<Long> longs = Sets.newHashSet();
        while (longs.size() < count) {
            longs.add(TestData.getLong());
        }
        for (long l : longs) {
            inventory.add(l);
            inventory.sync();
        }
        inventory = Inventory.create(backingStore);
        for (long l : longs) {
            Assert.assertTrue(inventory.contains(l));
        }
    }

}
