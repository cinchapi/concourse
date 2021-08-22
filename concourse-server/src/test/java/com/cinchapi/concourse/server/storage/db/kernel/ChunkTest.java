/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.storage.db.kernel.Chunk}
 * types.
 *
 * @author Jeff Nelson
 */
public abstract class ChunkTest<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends ConcourseBaseTest {

    protected BloomFilter filter;
    protected Chunk<L, K, V> chunk;
    protected String directory;
    protected Path file;
    private int pref = GlobalState.MAX_SEARCH_SUBSTRING_LENGTH;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            directory = TestData.DATA_DIR + File.separator + Time.now();
            FileSystem.mkdirs(directory);
            file = Paths.get(directory).resolve(UUID.randomUUID() + ".chunk");
            filter = BloomFilter.create(100);
            chunk = create(filter);
            // Don't allow dev preferences to interfere with unit test logic...
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = -1;
        }

        @Override
        protected void finished(Description description) {
            chunk = null;
            FileSystem.deleteDirectory(directory);
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = pref;
        }

        @Override
        protected void failed(Throwable e, Description description) {
            System.out.println(chunk.dump());
        }

    };

    @Test(expected = IllegalStateException.class)
    public void testCannotInsertInImmutableChunk() {
        chunk.insert(getLocator(), getKey(), getValue(), Time.now(),
                Action.ADD);
        chunk.transfer(Paths.get(FileOps.tempFile()));
        chunk.insert(getLocator(), getKey(), getValue(), Time.now(),
                Action.ADD);
    }

    @Test
    public void testMightContainLocatorKeyValue() {
        L locator = getLocator();
        K key = getKey();
        V value = getValue();
        Assert.assertFalse(
                chunk.mightContain(Composite.create(locator, key, value)));
        chunk.insert(locator, key, value, Time.now(), Action.ADD);
        Assert.assertTrue(
                chunk.mightContain(Composite.create(locator, key, value)));
    }

    @Test
    public void testSeekLocatorInMutableChunk() {

    }

    @Test
    public void testSeekLocatorInImmutableChunk() {

    }

    @Test
    public void testSeekLocatorAndKeyInMutableChunk() {

    }

    @Test
    public void testSeekLocatorAndKeyInImmutableChunk() {

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
                revision = chunk.makeRevision(locator, key, value, version,
                        type);
            }
            chunk.insert(revision.getLocator(), revision.getKey(),
                    revision.getValue(), revision.getVersion(),
                    revision.getType());
            revisions.add(revision);
        }
        chunk.transfer(file);
        chunk = load(file, filter, chunk.manifest());
        Iterator<Revision<L, K, V>> it = chunk.iterator();
        Set<Revision<L, K, V>> stored = Sets.newHashSetWithExpectedSize(count);
        while (it.hasNext()) {
            stored.add(it.next());
        }
        Assert.assertEquals(revisions, stored);
    }

    protected abstract L getLocator();

    protected abstract K getKey();

    protected abstract V getValue();

    protected abstract Chunk<L, K, V> create(BloomFilter filter);

    protected abstract Chunk<L, K, V> load(Path file, BloomFilter filter,
            Manifest manifest);

}
