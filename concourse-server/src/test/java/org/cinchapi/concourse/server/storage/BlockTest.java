/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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

import java.io.File;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.FileSystem;
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

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        private String directory;

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

    protected abstract L getLocator();

    protected abstract K getKey();

    protected abstract V getValue();

    protected abstract Block<L, K, V> getMutableBlock(String directory);

}
