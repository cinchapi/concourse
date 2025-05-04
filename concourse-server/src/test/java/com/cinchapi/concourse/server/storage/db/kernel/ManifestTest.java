/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
            Identifier key = Identifier.of(count);
            manifest.putStart(count, key);
            manifest.putEnd(count * 2, key);
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) manifest.length());
        manifest.flush(ByteSink.to(bytes));
        bytes.flip();
        FileSystem.writeBytes(bytes, file.toString());
        manifest = Manifest.load(file, 0, bytes.capacity());
        Assert.assertFalse(manifest.isLoaded());
        manifest.lookup(Identifier.of(1));
        Assert.assertTrue(manifest.isLoaded());
        for (int i = 0; i < count; i++) {
            Identifier key = Identifier.of(count);
            Range range = manifest.lookup(key);
            Assert.assertEquals(count, range.start());
            Assert.assertEquals(count * 2, range.end());
        }
    }

    @Test
    public void testManifestWorksAfterBeingFlushed() {
        int count = TestData.getScaleCount() * 2;
        Manifest manifest = Manifest.create(count);
        Identifier key = Identifier.of(count);
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
                Identifier record = Identifier.of(count);
                int $start = start;
                int end = start + TestData.getScaleCount();
                Range range = new Range() {

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

    @SuppressWarnings("rawtypes")
    @Test
    public void testBackgroundLoadEntriesWorksHit() {
        Set<Composite> composites = Sets.newLinkedHashSet();
        Manifest manifest = Manifest.create(100000);
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Composite composite = Composite.create(TestData.getText());
            if(composites.add(composite)) {
                manifest.putStart(i, composite);
                manifest.putEnd(i + 1, composite);
            }
        }
        Path file = Paths.get(TestData.getTemporaryTestFile());
        manifest.transfer(file);
        manifest = Manifest.load(file, 0,
                FileSystem.getFileSize(file.toString()));
        Composite composite = composites.iterator().next();
        Map map = Reflection.call(manifest, "entries", composite);
        Assert.assertTrue(map.containsKey(composite));
        Assert.assertEquals(1, map.size());
        while (Reflection.get("$entries", manifest) == null
                || Reflection.call(Reflection.get("$entries", manifest),
                        "get") == null) {/* spin */}

        map = Reflection.call(manifest, "entries", composite);
        Assert.assertTrue(map.containsKey(composite));
        Assert.assertEquals(composites.size(), map.size());
        map = Reflection.call(manifest, "entries");
        Assert.assertTrue(map.containsKey(composite));
        Assert.assertEquals(composites.size(), map.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testBackgroundLoadEntriesWorksMiss() {
        Set<Composite> composites = Sets.newLinkedHashSet();
        Manifest manifest = Manifest.create(100000);
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Composite composite = Composite.create(TestData.getText());
            if(composites.add(composite)) {
                manifest.putStart(i, composite);
                manifest.putEnd(i + 1, composite);
            }
        }
        Path file = Paths.get(TestData.getTemporaryTestFile());
        manifest.transfer(file);
        manifest = Manifest.load(file, 0,
                FileSystem.getFileSize(file.toString()));
        Composite composite = null;
        while (composite == null || composites.contains(composite)) {
            composite = Composite.create(TestData.getText());
        }
        Map map = Reflection.call(manifest, "entries", composite);
        Assert.assertFalse(map.containsKey(composite));
        Assert.assertTrue(map.isEmpty());
        while (Reflection.get("$entries", manifest) == null
                || Reflection.call(Reflection.get("$entries", manifest),
                        "get") == null) {/* spin */}

        map = Reflection.call(manifest, "entries", composite);
        Assert.assertFalse(map.containsKey(composite));
        Assert.assertEquals(composites.size(), map.size());
        map = Reflection.call(manifest, "entries");
        Assert.assertFalse(map.containsKey(composite));
        Assert.assertEquals(composites.size(), map.size());
    }

}
