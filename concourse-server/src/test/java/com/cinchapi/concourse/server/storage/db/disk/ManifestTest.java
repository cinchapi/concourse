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
package com.cinchapi.concourse.server.storage.db.disk;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Manifest}.
 *
 * @author Jeff Nelson
 */
public class ManifestTest extends ConcourseBaseTest {

    private Path file;

    @Rule
    public TestWatcher w = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            file = Paths.get(TestData.DATA_DIR + File.separator + Time.now());
        }

        @Override
        protected void finished(Description description) {
            if(FileSystem.hasFile(file)) {
                FileSystem.deleteFile(file.toString());
            }
        }

    };

    @Test(expected = IllegalStateException.class)
    public void testCannotModifyManifestAfterFreeze() {
        Manifest manifest = Manifest.create(1);
        manifest.putStart(0, TestData.getPosition());
        manifest.freeze(file);
        manifest.putEnd(1, TestData.getPosition());
    }

    @Test
    public void testManifestLoadsLazily() {
        int count = TestData.getScaleCount() * 2;
        Manifest manifest = Manifest.create(count);
        Assert.assertTrue(manifest.isLoaded());
        for (int i = 0; i < count; i++) {
            PrimaryKey key = PrimaryKey.wrap(count);
            manifest.putStart(count, key);
            manifest.putEnd(count * 2, key);
        }
        ByteBuffer bytes = manifest.getBytes();
        FileSystem.writeBytes(bytes, file.toString());
        manifest = Manifest.load(file, 0, bytes.capacity());
        Assert.assertFalse(manifest.isLoaded());
        manifest.getStart(PrimaryKey.wrap(1));
        Assert.assertTrue(manifest.isLoaded());
        for (int i = 0; i < count; i++) {
            PrimaryKey key = PrimaryKey.wrap(count);
            Assert.assertEquals(count, manifest.getStart(key));
            Assert.assertEquals(count * 2, manifest.getEnd(key));
        }
    }

    @Test
    public void testManifestWorksAfterBeingFrozen() {
        int count = TestData.getScaleCount() * 2;
        Manifest manifest = Manifest.create(count);
        PrimaryKey key = PrimaryKey.wrap(count);
        manifest.putStart(count, key);
        manifest.putEnd(count * 2, key);
        Assert.assertEquals(count, manifest.getStart(key));
        Assert.assertEquals(count * 2, manifest.getEnd(key));
        manifest.freeze(file);
        Assert.assertEquals(count, manifest.getStart(key));
        Assert.assertEquals(count * 2, manifest.getEnd(key));
    }

}
