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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.CorpusRecord;
import com.cinchapi.concourse.server.storage.db.IndexRecord;
import com.cinchapi.concourse.server.storage.db.TableRecord;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
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
            segment.transfer(TestData.getWriteAdd());
        }
        Assert.assertEquals(count, Iterators.size(segment.table().iterator()));
        Assert.assertEquals(count, Iterators.size(segment.index().iterator()));
    }

    @Test
    public void testSegmentSyncRoundTrip() throws SegmentLoadingException {
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            segment.transfer(TestData.getWriteAdd());
        }
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.fsync(file);
        Segment actual = Segment.load(file);
        Assert.assertEquals(segment.table(), actual.table());
        Assert.assertEquals(segment.index(), actual.index());
        Assert.assertEquals(segment.corpus(), actual.corpus());
    }

    @Test
    public void testRevisionTimestampTracking() {
        Write w0 = TestData.getWriteRemove();
        Write w1 = TestData.getWriteAdd();
        segment.transfer(w1);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w1.getVersion(), segment.maxTs);
        Write w2 = TestData.getWriteAdd();
        segment.transfer(w2);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w2.getVersion(), segment.maxTs);
        Write w4 = TestData.getWriteAdd();
        Write w3 = TestData.getWriteAdd();
        segment.transfer(w3);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.transfer(w4);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.transfer(w0);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
    }

    @Test
    public void testRevisionTimestampTrackingPersistence()
            throws SegmentLoadingException {
        Write w0 = TestData.getWriteRemove();
        Write w1 = TestData.getWriteAdd();
        segment.transfer(w1);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w1.getVersion(), segment.maxTs);
        Write w2 = TestData.getWriteAdd();
        segment.transfer(w2);
        Assert.assertEquals(w1.getVersion(), segment.minTs);
        Assert.assertEquals(w2.getVersion(), segment.maxTs);
        Write w4 = TestData.getWriteAdd();
        Write w3 = TestData.getWriteAdd();
        segment.transfer(w3);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.transfer(w4);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
        segment.transfer(w0);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.fsync(file);
        segment = Segment.load(file);
        Assert.assertEquals(w0.getVersion(), segment.minTs);
        Assert.assertEquals(w3.getVersion(), segment.maxTs);
    }

    @Test
    public void testSyncSegmentWithNoCorpusData()
            throws SegmentLoadingException {
        segment.transfer(Write.add("foo", Convert.javaToThrift(30), 1));
        Path file = Paths.get(TestData.getTemporaryTestFile());
        Assert.assertFalse(segment.corpus().iterator().hasNext());
        segment.fsync(file);
        segment = Segment.load(file);
        Assert.assertFalse(segment.corpus().iterator().hasNext());
        Assert.assertTrue(segment.table().iterator().hasNext());
        Assert.assertTrue(segment.index().iterator().hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotSyncEmptySegment() {
        Path file = Paths.get(TestData.getTemporaryTestFile());
        segment.fsync(file);
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
                PrimaryKey pk = PrimaryKey.wrap(record);
                Text text = Text.wrap(key);

                Thread reader = new Thread(() -> {
                    while (!done.get()) {
                        TableRecord tr = TableRecord.createPrimaryRecord(pk);
                        IndexRecord ir = IndexRecord
                                .createSecondaryRecord(text);
                        CorpusRecord cr = CorpusRecord.createSearchRecord(text);
                        segment.table().seek(Composite.create(pk), tr);
                        segment.index().seek(Composite.create(text), ir);
                        segment.corpus().seek(Composite.create(text), cr);
                        if(!done.get() && tr.isEmpty() != ir.isEmpty()) {
                            succeeded.set(false);
                            System.out.println(AnyStrings.format(
                                    "table empty = {} and index empty = {} for {}",
                                    tr.isEmpty(), ir.isEmpty(), record));
                        }
                        if(!done.get() && tr.isEmpty() != cr.isEmpty()) {
                            succeeded.set(false);
                            System.out.println(AnyStrings.format(
                                    "table empty = {} and corpus empty = {} for {}",
                                    tr.isEmpty(), cr.isEmpty(), record));
                        }
                    }
                    TableRecord tr = TableRecord.createPrimaryRecord(pk);
                    IndexRecord ir = IndexRecord.createSecondaryRecord(text);
                    CorpusRecord cr = CorpusRecord.createSearchRecord(text);
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
                        segment.transfer(write, executor);
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

}
