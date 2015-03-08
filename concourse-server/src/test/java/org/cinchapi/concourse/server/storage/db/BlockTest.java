/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.server.storage.db;

import java.io.File;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.db.Block;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class BlockTest<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends ConcourseBaseTest {

    protected Block<L, K, V> block;
    protected String directory;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            directory = TestData.DATA_DIR + File.separator + Time.now();
            block = getMutableBlock(directory);
        }

        @Override
        protected void finished(Description description) {
            block = null;
            FileSystem.deleteDirectory(directory);
        }

        @Override
        protected void failed(Throwable e, Description description) {
            System.out.println(block.dump());
        }
        
        

    };

    @Test(expected = IllegalStateException.class)
    public void testCannotInsertInImmutableBlock() {
        block.insert(getLocator(), getKey(), getValue(), Time.now(), Action.ADD);
        block.sync();
        block.insert(getLocator(), getKey(), getValue(), Time.now(), Action.ADD);
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
    
    @Test
    public final void testEquals(){
        String id = Long.toString(TestData.getLong());
        PrimaryBlock p = Block.createPrimaryBlock(id, directory + File.separator + "cpb");
        SecondaryBlock s = Block.createSecondaryBlock(id, directory + File.separator + "csb");
        SearchBlock t = Block.createSearchBlock(id, directory + File.separator + "ctb");
        Assert.assertEquals(p, s);
        Assert.assertEquals(p, t);
        Assert.assertEquals(s, t);
    }

    protected abstract L getLocator();

    protected abstract K getKey();

    protected abstract V getValue();

    protected abstract Block<L, K, V> getMutableBlock(String directory);

}
