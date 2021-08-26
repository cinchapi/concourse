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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.kernel.Manifest.Range;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;

/**
 * Unit tests for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.Manifest}.
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
    public void testCannotModifyManifestAfterTransfer() {
        Manifest manifest = Manifest.create(1);
        manifest.putStart(0, TestData.getPosition());
        manifest.transfer(file);
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
        ByteBuffer bytes = ByteBuffer.allocate((int) manifest.length());
        manifest.flush(ByteSink.to(bytes));
        bytes.flip();
        FileSystem.writeBytes(bytes, file.toString());
        manifest = Manifest.load(file, 0, bytes.capacity());
        Assert.assertFalse(manifest.isLoaded());
        manifest.lookup(PrimaryKey.wrap(1));
        Assert.assertTrue(manifest.isLoaded());
        for (int i = 0; i < count; i++) {
            PrimaryKey key = PrimaryKey.wrap(count);
            Range range = manifest.lookup(key);
            Assert.assertEquals(count, range.start());
            Assert.assertEquals(count * 2, range.end());
        }
    }

    @Test
    public void testManifestWorksAfterBeingFlushed() {
        int count = TestData.getScaleCount() * 2;
        Manifest manifest = Manifest.create(count);
        PrimaryKey key = PrimaryKey.wrap(count);
        manifest.putStart(count, key);
        manifest.putEnd(count * 2, key);
        Range range = manifest.lookup(key);
        Assert.assertEquals(count, range.start());
        Assert.assertEquals(count * 2, range.end());
        manifest.transfer(file);
        range = manifest.lookup(key);
        Assert.assertEquals(count, range.start());
        Assert.assertEquals(count * 2, range.end());
    }

    @Test
    public void testManifestStreamedEntriesAccuracy() {
        int threshold = Manifest.MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD;
        Manifest.MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD = (int) Math.pow(2,
                16);
        try {
            Manifest manifest = Manifest.create(100000);
            Text text = Text.wrap(Random.getString());
            int count = 0;
            int start = 0;
            Map<Composite, Range> expected = Maps.newHashMap();
            while (manifest
                    .length() < Manifest.MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD) {
                PrimaryKey record = PrimaryKey.wrap(count);
                int $start = start;
                int end = start + TestData.getScaleCount();
                Range range = new Manifest.Range() {

                    @Override
                    public long start() {
                        return $start;
                    }

                    @Override
                    public long end() {
                        return end;
                    }
                };
                Composite composite = Composite.create(text, record);
                manifest.putStart(start, composite);
                manifest.putEnd(end, composite);
                expected.put(composite, range);
                start = end + 1;
                ++count;
            }
            Path file = Paths.get(TestData.getTemporaryTestFile());
            manifest.transfer(file);
            Manifest $manifest = Manifest.load(file, 0,
                    FileSystem.getFileSize(file.toString()));
            expected.forEach((composite, range) -> {
                Range actual = $manifest.lookup(composite);
                Assert.assertEquals(range.start(), actual.start());
                Assert.assertEquals(range.end(), actual.end());
            });
        }
        finally {
            Manifest.MANIFEST_LENGTH_ENTRY_STREAMING_THRESHOLD = threshold;
        }

    }

}
