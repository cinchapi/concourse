/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.CorpusRecord;
import com.cinchapi.concourse.server.storage.db.IndexRecord;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRecord;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterators;

/**
 * Unit tests for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.Segment}.
 *
 * @author Jeff Nelson
 */
public class SegmentTest extends ConcourseBaseTest {

    private Segment segment;

    @Override
    protected void beforeEachTest() {
        segment = Segment.create();
    }

    @Test
    public void testAllWritesTransferredToTableAndIndexChunks() {
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            segment.acquire(TestData.getWriteAdd());
        }
        Assert.assertEquals(count, Iterators.size(segment.table().iterator()));
        Assert.assertEquals(count, Iterators.size(segment.index().iterator()));
    }

    @Test
    public void testSegmentSyncRoundTrip() throws SegmentLoadingException {
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            segment.acquire(TestData.getWriteAdd());
        }
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.transfer(file);
        Segment actual = Segment.load(file);
        Assert.assertEquals(segment.table(), actual.table());
        Assert.assertEquals(segment.index(), actual.index());
        Assert.assertEquals(segment.corpus(), actual.corpus());
    }

    @Test
    public void testRevisionTimestampTracking() {
        Write w0 = TestData.getWriteRemove();
        Write w1 = TestData.getWriteAdd();
        segment.acquire(w1);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w1.getVersion(), segment.maxTs);
        Write w2 = TestData.getWriteAdd();
        segment.acquire(w2);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w2.getVersion(), segment.maxTs);
        Write w4 = TestData.getWriteAdd();
        Write w3 = TestData.getWriteAdd();
        segment.acquire(w3);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.acquire(w4);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.acquire(w0);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
    }

    @Test
    public void testRevisionTimestampTrackingPersistence()
            throws SegmentLoadingException {
        Write w0 = TestData.getWriteRemove();
        Write w1 = TestData.getWriteAdd();
        segment.acquire(w1);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w1.getVersion(), segment.maxTs);
        Write w2 = TestData.getWriteAdd();
        segment.acquire(w2);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w2.getVersion(), segment.maxTs);
        Write w4 = TestData.getWriteAdd();
        Write w3 = TestData.getWriteAdd();
        segment.acquire(w3);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.acquire(w4);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.acquire(w0);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.transfer(file);
        segment = Segment.load(file);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
    }

    @Test
    public void testMemoryIsFreedAfterSync() {
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            segment.acquire(TestData.getInt() % 2 == 0 ? TestData.getWriteAdd()
                    : TestData.getWriteRemove());
        }
        Path file = Paths.get(TestData.getTemporaryTestFile());
        Assert.assertNotNull(Reflection.get("revisions", segment.table()));
        Assert.assertNotNull(Reflection.get("revisions", segment.index()));
        Assert.assertNotNull(Reflection.get("revisions", segment.corpus()));
        Assert.assertNotNull(
                Reflection.get("entries", segment.table().manifest()));
        Assert.assertNotNull(
                Reflection.get("entries", segment.index().manifest()));
        Assert.assertNotNull(
                Reflection.get("entries", segment.corpus().manifest()));
        segment.transfer(file);
        Assert.assertNull(Reflection.get("revisions", segment.table()));
        Assert.assertNull(Reflection.get("revisions", segment.index()));
        Assert.assertNull(Reflection.get("revisions", segment.corpus()));
        Assert.assertNull(
                Reflection.get("entries", segment.table().manifest()));
        Assert.assertNull(
                Reflection.get("entries", segment.index().manifest()));
        Assert.assertNull(
                Reflection.get("entries", segment.corpus().manifest()));
    }

    @Test
    public void testSyncSegmentWithNoCorpusData()
            throws SegmentLoadingException {
        segment.acquire(Write.add("foo", Convert.javaToThrift(30), 1));
        Path file = Paths.get(TestData.getTemporaryTestFile());
        Assert.assertFalse(segment.corpus().iterator().hasNext());
        segment.transfer(file);
        segment = Segment.load(file);
        Assert.assertFalse(segment.corpus().iterator().hasNext());
        Assert.assertTrue(segment.table().iterator().hasNext());
        Assert.assertTrue(segment.index().iterator().hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotSyncEmptySegment() {
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.transfer(file);
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        AtomicBoolean succeeded = new AtomicBoolean(true);
        AwaitableExecutorService executor = new AwaitableExecutorService(
                Executors.newCachedThreadPool());
        try {
            for (int i = 0; i < 1000; ++i) {
                AtomicBoolean done = new AtomicBoolean(false);
                long record = i;
                String key = Long.toString(record);
                TObject value = Convert.javaToThrift(Long.toString(record));
                Write write = Write.add(key, value, record);
                Identifier pk = Identifier.of(record);
                Text text = Text.wrap(key);

                Thread reader = new Thread(() -> {
                    while (!done.get()) {
                        TableRecord tr = TableRecord.create(pk);
                        IndexRecord ir = IndexRecord.create(text);
                        CorpusRecord cr = CorpusRecord.create(text);
                        segment.table().seek(Composite.create(pk), tr);
                        segment.index().seek(Composite.create(text), ir);
                        segment.corpus().seek(Composite.create(text), cr);
                        if(!done.get() && tr.isEmpty() != ir.isEmpty()) {
                            if(!tr.isEmpty() && ir.isEmpty()) {
                                // Later read is empty
                                succeeded.set(false);
                                System.out.println(AnyStrings.format(
                                        "table empty = {} and index empty = {} for {}",
                                        tr.isEmpty(), ir.isEmpty(), record));
                            }
                        }
                        if(!done.get() && ir.isEmpty() != cr.isEmpty()) {
                            if(!ir.isEmpty() && cr.isEmpty()) {
                                // Later read is empty
                                succeeded.set(false);
                                System.out.println(AnyStrings.format(
                                        "index empty = {} and corpus empty = {} for {}",
                                        tr.isEmpty(), cr.isEmpty(), record));
                            }
                        }
                    }
                    TableRecord tr = TableRecord.create(pk);
                    IndexRecord ir = IndexRecord.create(text);
                    CorpusRecord cr = CorpusRecord.create(text);
                    segment.table().seek(Composite.create(pk), tr);
                    segment.index().seek(Composite.create(text), ir);
                    segment.corpus().seek(Composite.create(text), cr);
                    if(tr.isEmpty()) {
                        succeeded.set(false);
                        System.out.println(
                                "After write finished, table still empty for "
                                        + record);
                    }
                    if(ir.isEmpty()) {
                        succeeded.set(false);
                        System.out.println(
                                "After write finished, index still empty for "
                                        + record);
                    }
                    if(cr.isEmpty()) {
                        succeeded.set(false);
                        System.out.println(
                                "After write finished, corpus still empty for "
                                        + record);
                    }

                });
                Thread writer = new Thread(() -> {
                    try {
                        segment.acquire(write, executor);
                        done.set(true);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                reader.start();
                writer.start();
                writer.join();
                reader.join();
            }
            Assert.assertTrue(succeeded.get());
        }
        finally {
            executor.shutdown();
        }
    }

    @Test
    public void testDataDeduplication() {
        String key = "name";
        TObject value = Convert.javaToThrift("Fonamey");
        long record = 1;

        // Simulate adding and removing while server is running, but creating
        // new intermediate TObjects
        Write w1 = Write.add(key, Convert.javaToThrift("Fonamey"), record);
        Write w2 = Write.remove(key, Convert.javaToThrift("Fonamey"), record);
        Assert.assertNotSame(w1.getValue(), w2.getValue());
        segment.acquire(w1);
        segment.acquire(w2);

        // Simulate loading data from disk and creating new intermediate because
        // values are not cached when read
        w1 = Write.fromByteBuffer(w1.getBytes());
        w2 = Write.fromByteBuffer(w2.getBytes());
        Assert.assertNotSame(w1.getValue(), w2.getValue());
        segment.acquire(w1);
        segment.acquire(w2);

        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Write write = Numbers.isEven(i) ? Write.remove(key, value, record)
                    : Write.add(key, value, record);
            write = Write.fromByteBuffer(write.getBytes());
            segment.acquire(write);
        }
        Text name = null;
        Value fonamey = null;
        Identifier one = null;
        Position position = null;
        Iterator<Revision<Identifier, Text, Value>> tit = segment.table()
                .iterator();
        while (tit.hasNext()) {
            Revision<Identifier, Text, Value> revision = tit.next();
            if(one == null) {
                one = revision.getLocator();
            }
            if(name == null) {
                name = revision.getKey();
            }
            if(fonamey == null) {
                fonamey = revision.getValue();
            }
            Assert.assertSame(one, revision.getLocator());
            Assert.assertSame(name, revision.getKey());
            Assert.assertSame(fonamey, revision.getValue());
        }
        Iterator<Revision<Text, Value, Identifier>> iit = segment.index()
                .iterator();
        while (iit.hasNext()) {
            Revision<Text, Value, Identifier> revision = iit.next();
            if(one == null) {
                one = revision.getValue();
            }
            if(name == null) {
                name = revision.getLocator();
            }
            if(fonamey == null) {
                fonamey = revision.getKey();
            }
            Assert.assertSame(one, revision.getValue());
            Assert.assertSame(name, revision.getLocator());
            Assert.assertSame(fonamey, revision.getKey());
        }
        Iterator<Revision<Text, Text, Position>> cit = segment.corpus()
                .iterator();
        while (cit.hasNext()) {
            Revision<Text, Text, Position> revision = cit.next();
            if(position == null) {
                position = revision.getValue();
            }
            if(name == null) {
                name = revision.getLocator();
            }
            Assert.assertSame(position, revision.getValue());
            Assert.assertSame(name, revision.getLocator());
            if(revision.getKey().toString().equals("name")) {
                Assert.assertSame(name, revision.getKey());
            }

        }
    }

    @Test
    public void testBloomFilterAccuracyOnLoad() throws SegmentLoadingException {
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            segment.acquire(TestData.getWriteAdd());
        }
        BloomFilter table = segment.table().filter();
        BloomFilter index = segment.index().filter();
        BloomFilter corpus = segment.corpus().filter();
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.transfer(file);
        segment = Segment.load(file);
        Assert.assertEquals(table, segment.table().filter());
        Assert.assertEquals(index, segment.index().filter());
        Assert.assertEquals(corpus, segment.corpus().filter());
    }

    @Test
    public void testOffHeapSegmentEfficacy() {
        segment = Segment.createOffHeap(100);
        Set<Write> writes = new LinkedHashSet<>();
        for (int i = 0; i < 100; ++i) {
            Write write = TestData.getWriteAdd();
            writes.add(write);
            segment.acquire(write);
        }
        Set<Write> actual = (Set<Write>) segment.writes()
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Assert.assertEquals(writes, actual);
    }

}
