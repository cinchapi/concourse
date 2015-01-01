/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.util.Set;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link Inventory} class.
 * 
 * @author jnelson
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
