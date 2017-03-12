/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.temp;

import java.io.File;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.cinchapi.common.base.TernaryTruth;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.PermanentStore;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Limbo;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Buffer}.
 *
 * @author Jeff Nelson
 */
public class BufferTest extends LimboTest {

    private static PermanentStore MOCK_DESTINATION = Mockito
            .mock(PermanentStore.class);
    static {
        // NOTE: The Buffer assumes it is transporting to a Database, but we
        // cannot mock that class with Mockito since it is final. Mocking the
        // PermanentStore interface does not pose a problem as long as tests
        // don't do something that would cause the Database#triggerSync() method
        // to be called (i.e. transporting more than a page worth of Writes).
        //
        // So, please use the Buffer#canTransport() method to check to see if is
        // okay to do a transport without causing a triggerSync(). And do not
        // unit tests streaming writes in this test class (do that at a level
        // above where an actual Database is defined)!!!
        Mockito.doNothing().when(MOCK_DESTINATION)
                .accept(Mockito.any(Write.class));
    }

    private String current;

    @Override
    protected Buffer getStore() {
        current = TestData.DATA_DIR + File.separator + Time.now();
        return new Buffer(current);
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(current);
    }

    @Test
    public void testBufferCanAddPageWhileServicingRead()
            throws InterruptedException {
        int count = 0;
        while (!((Buffer) store).canTransport()) {
            add("foo", Convert.javaToThrift(count), 1);
            count++;
        }
        // Now add a second page worth of writes, but but don't spill over into
        // a third page yet
        int max = 0;
        for (int i = count; i < (count * 2) - 2; i++) {
            add("foo", Convert.javaToThrift(i), 1);
            max = i;
        }
        final int value = max + 1;
        final AtomicBoolean caughtException = new AtomicBoolean(false);
        Thread read = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    store.select("foo", 1);
                }
                catch (ConcurrentModificationException e) {
                    caughtException.set(true);
                }
            }

        });
        Thread write = new Thread(new Runnable() {

            @Override
            public void run() {
                add("foo", Convert.javaToThrift(value + 1), 1);
            }

        });
        read.start();
        write.start();
        write.join();
        read.join();
        Assert.assertFalse(caughtException.get());
    }

    @Test
    public void testIteratorAfterTransport() {
        ((Buffer) store).transportRateMultiplier = 1;
        List<Write> writes = getWrites();
        int j = 0;
        for (Write write : writes) {
            add(write.getKey().toString(), write.getValue().getTObject(), write
                    .getRecord().longValue());
            Variables.register("write_" + j, write);
            j++;
        }
        Variables.register("size_pre_transport", writes.size());
        int div = Variables.register("div", (TestData.getScaleCount() % 9) + 1);
        int count = Variables.register("count", writes.size() / div);
        for (int i = 0; i < count; i++) {
            if(((Buffer) store).canTransport()) {
                ((Buffer) store).transport(MOCK_DESTINATION);
                writes.remove(0);
            }
            else {
                break;
            }
        }
        Variables.register("size_post_transport", writes.size());
        Iterator<Write> it0 = ((Limbo) store).iterator();
        Iterator<Write> it1 = writes.iterator();
        while (it1.hasNext()) {
            Assert.assertTrue(it0.hasNext());
            Write w0 = it0.next();
            Write w1 = it1.next();
            Assert.assertEquals(w0, w1);
        }
        Assert.assertFalse(it0.hasNext());
    }

    @Test
    public void testWaitUntilTransportable() throws InterruptedException {
        final AtomicLong later = new AtomicLong(0);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                ((Buffer) store).waitUntilTransportable();
                later.set(Time.now());
            }

        });
        thread.start();
        long before = Time.now();
        while (!((Buffer) store).canTransport()) {
            before = Time.now();
            add(TestData.getString(), TestData.getTObject(), TestData.getLong());
        }
        thread.join(); // make sure thread finishes before comparing
        Assert.assertTrue(later.get() > before);
    }

    @Test
    @Ignore
    public void testOnDiskIterator() {
        Buffer buffer = (Buffer) store;
        int count = TestData.getScaleCount();
        List<Write> expected = Lists.newArrayList();
        for (int i = 0; i < count; ++i) {
            Write write = Write.add(TestData.getSimpleString(),
                    TestData.getTObject(), i);
            buffer.insert(write);
            expected.add(write);
            Variables.register("expected_" + i, write);
        }
        buffer.stop();
        Iterator<Write> it = Buffer.onDiskIterator(buffer.getBackingStore());
        List<Write> stored = Lists.newArrayList();
        int i = 0;
        while (it.hasNext()) {
            Write write = it.next();
            stored.add(write);
            Variables.register("actual_" + i, write);
            ++i;
        }
        Assert.assertEquals(expected, stored);
    }

    @Test
    public void testVerifyFastTrue() {
        Buffer buffer = (Buffer) store;
        Write write = Write.add("foo", Convert.javaToThrift("bar"), 1);
        buffer.insert(write);
        Assert.assertEquals(TernaryTruth.TRUE, buffer.verifyFast(write));
    }

    @Test
    public void testVerifyFastFalseRemoved() {
        Buffer buffer = (Buffer) store;
        Write write = Write.add("foo", Convert.javaToThrift("bar"), 1);
        buffer.insert(write);
        buffer.insert(write.inverse());
        Assert.assertEquals(TernaryTruth.FALSE, buffer.verifyFast(write));
    }

    @Test
    public void testVerifyFastFalseNeverAdded() {
        Buffer buffer = (Buffer) store;
        Write write = Write.add("foo", Convert.javaToThrift("bar"), 1);
        Assert.assertEquals(TernaryTruth.FALSE, buffer.verifyFast(write));
    }

    @Test
    public void testVerifyFastUnsure() {
        Buffer buffer = (Buffer) store;
        Write write = Write.add("foo", Convert.javaToThrift("bar"), 1);
        buffer.insert(write);
        while (!buffer.canTransport()) {
            buffer.insert(TestData.getWriteAdd());
        }
        buffer.transport(MOCK_DESTINATION);
        Assert.assertEquals(TernaryTruth.UNSURE, buffer.verifyFast(write));
    }

    @Test
    public void testOnDiskIteratorEmptyDirectory() {
        Buffer buffer = (Buffer) store;
        Buffer.onDiskIterator(buffer.getBackingStore() + "/foo").hasNext();
        Assert.assertTrue(true); // lack of exception means test passes
    }

    @Test
    public void testPageExpansion() {
        // NOTE: This test is designed to ensure that buffer pages can
        // automatically expand to accommodate a write that is larger than
        // BUFFER_PAGE_SIZE
        int oldBufferPageSize = GlobalState.BUFFER_PAGE_SIZE;
        try {
            GlobalState.BUFFER_PAGE_SIZE = 4;
            Buffer buffer = getStore();
            buffer.start();
            buffer.insert(Write.add("foo", Convert.javaToThrift(4), 1));
            Assert.assertTrue(buffer.contains(1));
        }
        finally {
            GlobalState.BUFFER_PAGE_SIZE = oldBufferPageSize;
        }
    }

    @Test
    public void testPercentVerifyScansAllWrites() {
        Buffer buffer = (Buffer) store;
        List<Write> stored = addRandomElementsToBufferAndList(buffer,
                TestData.getScaleCount());
        for (Write write : stored) {
            buffer.verify(write.getKey().toString(), write.getValue()
                    .getTObject(), write.getRecord().longValue());
        }
        float percent = Reflection.call(buffer, "getPercentVerifyScans");
        Assert.assertEquals(1.0f, percent, 0f);
    }

    @Test
    public void testPercentVerifyScansSomeWrites() {
        Buffer buffer = (Buffer) store;
        int stored = 0;
        int count = TestData.getScaleCount();
        Set<Write> writes = Sets.newHashSet();
        for (int i = 0; i < count; ++i) {
            int seed = TestData.getInt();
            Write write = null;
            while (write == null || !writes.add(write)) {
                write = TestData.getWriteAdd();
            }
            if(seed % 3 == 0) {
                buffer.insert(write);
                ++stored;
            }
            writes.add(write);
        }
        for (Write write : writes) {
            buffer.verify(write.getKey().toString(), write.getValue()
                    .getTObject(), write.getRecord().longValue());
        }
        float percentVerifyScans = Reflection.call(buffer,
                "getPercentVerifyScans");
        float floor = (float) stored / writes.size();
        Assert.assertTrue(percentVerifyScans >= floor);
    }

    @Test
    public void testAsyncWritesToBuffer() {
        GlobalState.BINARY_QUEUE.clear();
        Buffer buffer = (Buffer) store;
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Write write = null;
            while (write == null) {
                write = TestData.getWriteAdd();
            }
            buffer.insert(write, true);
        }
        while (GlobalState.BINARY_QUEUE.size() < count) {
            // wait for all the async placements onto the binary queue to finish
            continue;
        }
        Assert.assertEquals(count, GlobalState.BINARY_QUEUE.size());
        GlobalState.BINARY_QUEUE.clear();
    }

    /**
     * Helper method used by multiple test cases to add a random number of
     * random elements to
     * the {@link Buffer} and a {@code List<Write>}, and returns the list.
     *
     * @param buff: the buffer into which objects are inserted
     * @param size: the number of objects to insert
     * @return: a {@code List} of {@link Write} objects that were also inserted
     *          into the buffer
     */
    private List<Write> addRandomElementsToBufferAndList(Buffer buff, int size) {
        List<Write> stored = Lists.newArrayList();
        for (int i = 0; i < size; ++i) {
            Write write = Write.add(TestData.getSimpleString(),
                    TestData.getTObject(), TestData.getLong());
            buff.insert(write);
            stored.add(write);
        }
        return stored;
    }
}
