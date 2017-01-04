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
package com.cinchapi.concourse.server.storage.db;

import java.io.File;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.storage.db.BlockIndex;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link BlockIndex}.
 * 
 * @author Jeff Nelson
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

    @Test
    public void testBlockWorksAfterBeingSynced() {
        // basically check that we can sync to disk and the block index still
        // works fine
        int count = TestData.getScaleCount() * 2;
        BlockIndex index = BlockIndex.create(file, count);
        PrimaryKey key = PrimaryKey.wrap(count);
        index.putStart(count, key);
        index.putEnd(count * 2, key);
        Assert.assertEquals(count, index.getStart(key));
        Assert.assertEquals(count * 2, index.getEnd(key));
        index.sync();
        Assert.assertEquals(count, index.getStart(key));
        Assert.assertEquals(count * 2, index.getEnd(key));
    }

}
