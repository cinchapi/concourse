/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.db;

import java.io.File;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Unit tests for {@link BlockIndex}.
 * 
 * @author jnelson
 */
public class BlockIndexTest extends ConcourseBaseTest {

    private String file;

    @Rule
    public TestWatcher w = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            file = TestData.DATA_DIR + File.separator + Time.now();
        }

        @Override
        protected void finished(Description description) {
            if(FileSystem.hasFile(file)) {
                FileSystem.deleteFile(file);
            }
        }

    };

    @Test(expected = IllegalStateException.class)
    public void testCannotModifyBlockIndexAfterSync() {
        BlockIndex index = BlockIndex.create(file, 1);
        index.putStart(0, TestData.getPosition());
        index.sync();
        index.putEnd(1, TestData.getPosition());
    }

    @Test
    public void testBlockIndexLoadsLazily() {
        int count = TestData.getScaleCount() * 2;
        BlockIndex index = BlockIndex.create(file, count);
        Assert.assertTrue(index.isLoaded());
        for (int i = 0; i < count; i++) {
            PrimaryKey key = PrimaryKey.wrap(count);
            index.putStart(count, key);
            index.putEnd(count * 2, key);
        }
        index.sync();
        index = BlockIndex.open(file);
        Assert.assertFalse(index.isLoaded());
        index.getStart(PrimaryKey.wrap(1));
        Assert.assertTrue(index.isLoaded());
        for (int i = 0; i < count; i++) {
            PrimaryKey key = PrimaryKey.wrap(count);
            Assert.assertEquals(count, index.getStart(key));
            Assert.assertEquals(count * 2, index.getEnd(key));
        }
    }

}
