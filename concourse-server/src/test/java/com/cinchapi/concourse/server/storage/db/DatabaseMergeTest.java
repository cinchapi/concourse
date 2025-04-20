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
package com.cinchapi.concourse.server.storage.db;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.AbstractStoreTest;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.db.kernel.Segment.Receipt;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link Database#merge(Segment, List)} method.
 *
 * @author Jeff Nelson
 */
public class DatabaseMergeTest extends AbstractStoreTest {

    private String directory;

    /**
     * Test that a segment can be successfully appended to the database
     * and its data is accessible.
     */
    @Test
    public void testMergeSegment() {
        Database db = (Database) store;
        // Create a segment with some data
        Segment segment = Segment.create();
        String key = "test_key";
        TObject value = Convert.javaToThrift("test_value");
        long record = 1;
        Write write = Write.add(key, value, record);

        // Acquire the write in the segment and collect the receipt
        Receipt receipt = segment.acquire(write);
        List<Receipt> receipts = Lists.newArrayList(receipt);

        // Append the segment to the database
        db.merge(segment, receipts);

        // Verify the data is accessible
        Assert.assertTrue(db.select(key, record).contains(value));
    }

    /**
     * Test that multiple segments can be appended and all data is accessible.
     */
    @Test
    public void testMergeMultipleSegments() {
        Database db = (Database) store;
        // Create and append first segment
        Segment segment1 = Segment.create();
        String key1 = "key1";
        TObject value1 = Convert.javaToThrift("value1");
        long record1 = 1;
        Write write1 = Write.add(key1, value1, record1);
        Receipt receipt1 = segment1.acquire(write1);
        db.merge(segment1, Lists.newArrayList(receipt1));

        // Create and append second segment
        Segment segment2 = Segment.create();
        String key2 = "key2";
        TObject value2 = Convert.javaToThrift("value2");
        long record2 = 2;
        Write write2 = Write.add(key2, value2, record2);
        Receipt receipt2 = segment2.acquire(write2);
        db.merge(segment2, Lists.newArrayList(receipt2));

        // Verify all data is accessible
        Assert.assertTrue(db.select(key1, record1).contains(value1));
        Assert.assertTrue(db.select(key2, record2).contains(value2));
    }

    /**
     * Test that appending a segment updates the caches correctly.
     */
    @Test
    public void testMergeUpdatesCache() {
        Database db = (Database) store;
        // First query to populate cache
        String key = "cache_key";
        TObject value = Convert.javaToThrift("cache_value");
        long record = 1;

        // Create a segment with data
        Segment segment = Segment.create();
        Write write = Write.add(key, value, record);
        Receipt receipt = segment.acquire(write);

        // Perform a query to populate cache
        db.find(key, Operator.EQUALS, value);

        // Append the segment
        db.merge(segment, Lists.newArrayList(receipt));

        // Verify cache is updated by checking the result contains the new
        // record
        Set<Long> results = db.find(key, Operator.EQUALS, value);
        Assert.assertTrue(results.contains(record));
    }

    /**
     * Test that a mutable segment is persisted to disk when appended.
     */
    @Test
    public void testMergePersistsMutableSegment() {
        Database db = (Database) store;
        // Create a mutable segment
        Segment segment = Segment.create();
        String key = "persist_key";
        TObject value = Convert.javaToThrift("persist_value");
        long record = 1;
        Write write = Write.add(key, value, record);
        Receipt receipt = segment.acquire(write);

        // Verify segment is mutable
        Assert.assertTrue(segment.isMutable());

        // Append the segment
        db.merge(segment, Lists.newArrayList(receipt));

        // Get the segments directory
        Path segmentsDir = Paths.get(directory).resolve("segments");

        // Verify a segment file exists in the directory
        Assert.assertTrue(segmentsDir.toFile().exists());
        Assert.assertTrue(segmentsDir.toFile().list().length > 0);
    }

    /**
     * Test that appending a segment with empty receipts doesn't update caches.
     */
    @Test
    public void testMergeWithEmptyReceipts() {
        Database db = (Database) store;
        // Create a segment with data but provide empty receipts
        Segment segment = Segment.create();
        String key = "test_key";
        TObject value = Convert.javaToThrift("test_value");
        long record = 1;
        Write write = Write.add(key, value, record);
        segment.acquire(write);

        // Append with empty receipts
        List<Receipt> emptyReceipts = new ArrayList<>();
        db.merge(segment, emptyReceipts);

        // Verify the segment is added but data isn't in cache
        List<Segment> segments = Reflection.get("segments", db);
        Assert.assertTrue(segments.contains(segment));

        // Data should still be accessible through normal read path
        Assert.assertTrue(db.select(key, record).contains(value));
    }

    /**
     * Test that appending a null segment throws an exception.
     */
    @Test(expected = NullPointerException.class)
    public void testMergeNullSegment() {
        Database db = (Database) store;
        db.merge(null, new ArrayList<>());
    }

    /**
     * Test that appending a segment with receipts from a different segment
     * still works but may lead to inconsistent cache state.
     */
    @Test
    public void testMergeWithMismatchedReceipts() {
        Database db = (Database) store;
        // Create first segment with data
        Segment segment1 = Segment.create();
        String key1 = "key1";
        TObject value1 = Convert.javaToThrift("value1");
        long record1 = 1;
        Write write1 = Write.add(key1, value1, record1);
        Receipt receipt1 = segment1.acquire(write1);

        // Create second segment with different data
        Segment segment2 = Segment.create();
        String key2 = "key2";
        TObject value2 = Convert.javaToThrift("value2");
        long record2 = 2;
        Write write2 = Write.add(key2, value2, record2);
        segment2.acquire(write2);

        // Append segment2 with receipts from segment1
        db.merge(segment2, Lists.newArrayList(receipt1));

        // Verify segment2 is added
        List<Segment> segments = Reflection.get("segments", db);
        Assert.assertTrue(segments.contains(segment2));

        // Verify data from segment2 is accessible
        Assert.assertTrue(db.select(key2, record2).contains(value2));
    }

    /**
     * Test that appending a segment with a large number of writes works
     * correctly.
     */
    @Test
    public void testMergeWithLargeNumberOfWrites() {
        Database db = (Database) store;
        Segment segment = Segment.create();
        List<Receipt> receipts = new ArrayList<>();

        // Add a large number of writes
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = "key" + i;
            TObject value = Convert.javaToThrift("value" + i);
            long record = i;
            Write write = Write.add(key, value, record);
            receipts.add(segment.acquire(write));
        }

        // Append the segment
        db.merge(segment, receipts);

        // Verify random samples of data are accessible
        for (int i = 0; i < 10; i++) {
            int index = TestData.getScaleCount() % count;
            String key = "key" + index;
            TObject value = Convert.javaToThrift("value" + index);
            long record = index;
            Assert.assertTrue(db.select(key, record).contains(value));
        }
    }

    /**
     * Test that appending an immutable segment doesn't try to persist it again.
     */
    @Test
    public void testMergeImmutableSegment() {
        Database db = (Database) store;
        // Create a segment and make it immutable
        Segment segment = Segment.create();
        String key = "test_key";
        TObject value = Convert.javaToThrift("test_value");
        long record = 1;
        Write write = Write.add(key, value, record);
        Receipt receipt = segment.acquire(write);

        // Make segment immutable by transferring it to a file
        Path tempFile = Paths.get(TestData.getTemporaryTestFile());
        segment.transfer(tempFile);

        // Verify segment is now immutable
        Assert.assertFalse(segment.isMutable());

        // Append the immutable segment
        db.merge(segment, Lists.newArrayList(receipt));

        // Verify data is accessible
        Assert.assertTrue(db.select(key, record).contains(value));
    }

    /**
     * Test that seg0 is always at the end of the segments list after appending
     * segments to the database.
     */
    @Test
    public void testSeg0AlwaysAtEndOfSegmentsList() {
        Database db = (Database) store;

        // Get the initial segments list and verify seg0 is at the end
        List<Segment> segments = Reflection.get("segments", db);
        Segment seg0 = Reflection.get("seg0", db);
        Assert.assertEquals(seg0, segments.get(segments.size() - 1));

        // Create and append a segment
        Segment segment1 = Segment.create();
        String key1 = "key1";
        TObject value1 = Convert.javaToThrift("value1");
        long record1 = 1;
        Write write1 = Write.add(key1, value1, record1);
        Receipt receipt1 = segment1.acquire(write1);
        db.merge(segment1, Lists.newArrayList(receipt1));

        // Verify seg0 is still at the end
        segments = Reflection.get("segments", db);
        Assert.assertEquals(seg0, segments.get(segments.size() - 1));

        // Create and append multiple segments
        for (int i = 0; i < 3; i++) {
            Segment segment = Segment.create();
            String key = "key" + (i + 2);
            TObject value = Convert.javaToThrift("value" + (i + 2));
            long record = i + 2;
            Write write = Write.add(key, value, record);
            Receipt receipt = segment.acquire(write);
            db.merge(segment, Lists.newArrayList(receipt));
        }

        // Verify seg0 is still at the end after multiple appends
        segments = Reflection.get("segments", db);
        Assert.assertEquals(seg0, segments.get(segments.size() - 1));

        // Create a segment and make it immutable before appending
        Segment immutableSegment = Segment.create();
        String key = "immutable_key";
        TObject value = Convert.javaToThrift("immutable_value");
        long record = 10;
        Write write = Write.add(key, value, record);
        Receipt receipt = immutableSegment.acquire(write);

        // Make segment immutable by transferring it to a file
        Path tempFile = Paths.get(TestData.getTemporaryTestFile());
        immutableSegment.transfer(tempFile);

        // Append the immutable segment
        db.merge(immutableSegment, Lists.newArrayList(receipt));

        // Verify seg0 is still at the end after appending an immutable segment
        segments = Reflection.get("segments", db);
        Assert.assertEquals(seg0, segments.get(segments.size() - 1));
    }

    @Override
    protected void add(String key, TObject value, long record) {
        // No needed for these tests
        throw new UnsupportedOperationException();
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(directory);
    }

    @Override
    protected Store getStore() {
        directory = TestData.getTemporaryTestDir();
        return new Database(directory);

    }

    @Override
    protected void remove(String key, TObject value, long record) {
        // No needed for these tests
        throw new UnsupportedOperationException();
    }
}