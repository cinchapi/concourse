/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public abstract class BlockTest<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends ConcourseBaseTest {

    protected Block<L, K, V> block;
    protected String directory;
    private int pref = GlobalState.MAX_SEARCH_SUBSTRING_LENGTH;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            directory = TestData.DATA_DIR + File.separator + Time.now();
            block = getMutableBlock(directory);
            // Don't allow dev preferences to interfere with unit test logic...
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = -1;
        }

        @Override
        protected void finished(Description description) {
            block = null;
            FileSystem.deleteDirectory(directory);
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = pref;
        }

        @Override
        protected void failed(Throwable e, Description description) {
            System.out.println(block.dump());
        }

    };

    @Test(expected = IllegalStateException.class)
    public void testCannotInsertInImmutableBlock() {
        block.insert(getLocator(), getKey(), getValue(), Time.now(),
                Action.ADD);
        block.sync();
        block.insert(getLocator(), getKey(), getValue(), Time.now(),
                Action.ADD);
    }

    @Test
    public void testMightContainLocatorKeyValue() {
        L locator = getLocator();
        K key = getKey();
        V value = getValue();
        Assert.assertFalse(block.mightContain(locator, key, value));
        block.insert(locator, key, value, Time.now(), Action.ADD);
        Assert.assertTrue(block.mightContain(locator, key, value));
    }

    @Test
    public void testSeekLocatorInMutableBlock() {

    }

    @Test
    public void testSeekLocatorInImmutableBlock() {

    }

    @Test
    public void testSeekLocatorAndKeyInMutableBlock() {

    }

    @Test
    public void testSeekLocatorAndKeyInImmutableBlock() {

    }

    @Test(expected = IllegalStateException.class)
    public void testCannotGetIteratorForMutableBlock() {
        block.insert(getLocator(), getKey(), getValue(), Time.now(),
                Action.ADD);
        block.iterator();
    }

    @Test
    public void testIterator() {
        int count = TestData.getScaleCount();
        Set<Revision<L, K, V>> revisions = Sets
                .newHashSetWithExpectedSize(count);
        for (int i = 0; i < count; ++i) {
            Revision<L, K, V> revision = null;
            while (revision == null || revisions.contains(revision)) {
                L locator = getLocator();
                K key = getKey();
                V value = getValue();
                long version = Time.now();
                Action type = Action.ADD;
                revision = block.makeRevision(locator, key, value, version,
                        type);
            }
            block.insert(revision.getLocator(), revision.getKey(),
                    revision.getValue(), revision.getVersion(),
                    revision.getType());
            revisions.add(revision);
        }
        block.sync();
        Iterator<Revision<L, K, V>> it = block.iterator();
        Set<Revision<L, K, V>> stored = Sets.newHashSetWithExpectedSize(count);
        while (it.hasNext()) {
            stored.add(it.next());
        }
        Assert.assertEquals(revisions, stored);
    }

    @Test
    public final void testEquals() {
        String id = Long.toString(TestData.getLong());
        PrimaryBlock p = Block.createPrimaryBlock(id,
                directory + File.separator + "cpb");
        SecondaryBlock s = Block.createSecondaryBlock(id,
                directory + File.separator + "csb");
        SearchBlock t = Block.createSearchBlock(id,
                directory + File.separator + "ctb");
        Assert.assertEquals(p, s);
        Assert.assertEquals(p, t);
        Assert.assertEquals(s, t);
    }

    @Test
    public void testStatsSchemaVersion() {
        Assert.assertEquals((long) Block.SCHEMA_VERSION,
                (long) block.stats().get(Attribute.SCHEMA_VERSION));
    }

    @Test
    public void testRevisionVersionTracking() {
        long version1 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version1, Action.ADD);
        Assert.assertEquals(version1,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version1,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
        long version2 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version2, Action.ADD);
        Assert.assertEquals(version2,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version1,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
        long version4 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version4, Action.ADD);
        Assert.assertEquals(version4,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version1,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
        long version3 = version4 - 1;
        block.insert(getLocator(), getKey(), getValue(), version3, Action.ADD);
        Assert.assertEquals(version4,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version1,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
        long version0 = version1 - 1;
        block.insert(getLocator(), getKey(), getValue(), version0, Action.ADD);
        Assert.assertEquals(version4,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version0,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
        block.sync();
        Assert.assertEquals(version4,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version0,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRevisionVersionTrackingPersistence() {
        long version1 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version1, Action.ADD);

        long version2 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version2, Action.ADD);

        long version4 = Time.now();
        block.insert(getLocator(), getKey(), getValue(), version4, Action.ADD);

        long version3 = version4 - 1;
        block.insert(getLocator(), getKey(), getValue(), version3, Action.ADD);

        long version0 = version1 - 1;
        block.insert(getLocator(), getKey(), getValue(), version0, Action.ADD);

        block.sync();
        block = Reflection.newInstance(block.getClass(), block.getId(),
                directory, true); /* (Authorized) */
        Assert.assertEquals(version4,
                (long) block.stats().get(Attribute.MAX_REVISION_VERSION));
        Assert.assertEquals(version0,
                (long) block.stats().get(Attribute.MIN_REVISION_VERSION));
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotGetChecksumOfMutableBlock() {
        String directory = TestData.getTemporaryTestDir();
        Block<L, K, V> block = getMutableBlock(directory);
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            block.insert(getLocator(), getKey(), getValue(), Time.now(),
                    Action.ADD);
        }
        block.checksum();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testChecksumSameAfterSyncAndWhenLoaded() {
        String directory = TestData.getTemporaryTestDir();
        Block<L, K, V> block = getMutableBlock(directory);
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            block.insert(getLocator(), getKey(), getValue(), Time.now(),
                    Action.ADD);
        }
        block.sync();
        String expected = block.checksum();
        block = Reflection.newInstance(block.getClass(), block.getId(),
                directory, true);
        String actual = block.checksum();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReindex() {
        String directory = TestData.getTemporaryTestDir();
        Block<L, K, V> block = getMutableBlock(directory);
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            block.insertUnsafe(getLocator(), getKey(), getValue(), Time.now(),
                    Action.ADD);
        }
        block.sync();
        BlockIndex aindex = Reflection.get("index", block); // (authorized)
        BloomFilter afilter = Reflection.get("filter", block); // (authorized)
        block.reindex();
        BlockIndex bindex = Reflection.get("index", block); // (authorized)
        BloomFilter bfilter = Reflection.get("filter", block); // (authorized)
        Assert.assertNotSame(aindex, bindex);
        Assert.assertEquals(aindex, bindex);
        Assert.assertNotSame(afilter, bfilter);
        Assert.assertEquals(afilter, bfilter);
        // TODO: check stats
    }

    protected abstract L getLocator();

    protected abstract K getKey();

    protected abstract V getValue();

    protected abstract Block<L, K, V> getMutableBlock(String directory);

}
