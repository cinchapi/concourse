/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link SharedMemory} class.
 *
 * @author Jeff Nelson
 */
public class SharedMemoryTest {

    @Test
    public void testBasicWrite() {
        SharedMemory queue = new SharedMemory();
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBasicRead() {
        String location = FileOps.tempFile();
        SharedMemory queue = new SharedMemory(location);
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        SharedMemory queue2 = new SharedMemory(location);
        Assert.assertNotEquals(queue, queue2);
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCompaction() {
        int toRead = Random.getScaleCount();
        int total = toRead + Random.getScaleCount();
        SharedMemory memory = new SharedMemory();
        List<String> expected = Lists.newArrayList();
        for (int i = 0; i < total; ++i) {
            String message = Random.getString();
            memory.write(ByteBuffers.fromString(message));
            expected.add(message);
        }
        List<String> actual = Lists
                .newArrayListWithExpectedSize(expected.size());
        for (int i = 0; i < toRead; ++i) {
            String message = ByteBuffers.getString(memory.read());
            actual.add(message);
        }
        int pos0 = ((StoredInteger) Reflection.get("nextRead", memory)).get();
        memory.compact();
        int pos1 = ((StoredInteger) Reflection.get("nextRead", memory)).get();
        Assert.assertTrue(pos0 > pos1);
        for (int i = toRead; i < total; ++i) {
            String message = ByteBuffers.getString(memory.read());
            actual.add(message);
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCompactionWhenEmpty() {
        SharedMemory memory = new SharedMemory();
        memory.compact();
        Assert.assertTrue(true); // lack of exception means test passes
    }

    @Test
    public void testCompactionDecreasesUnderlyingFileSize() {
        String file = FileOps.tempFile();
        SharedMemory memory = new SharedMemory(file);
        try {
            memory.write(ByteBuffers.fromString("hello world"));
            long size = Files.size(Paths.get(file));
            memory.read();
            memory.compact();
            Assert.assertTrue(size > Files.size(Paths.get(file)));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByRead() {
        SharedMemory memory = new SharedMemory();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        Assert.assertEquals(ByteBuffers.fromString("ccccc"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByWriteRead() {
        SharedMemory memory = new SharedMemory();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        memory.write(ByteBuffers.fromString("ff"));
        Assert.assertEquals(ByteBuffers.fromString("ccccc"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("ff"), memory.read());
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByReadWriteRead() {
        SharedMemory memory = new SharedMemory();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        memory.read();
        memory.write(ByteBuffers.fromString("ff"));
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("ff"), memory.read());
    }

    @Test
    public void testCompactionIsReflectedAcrossInstances() {
        String file = FileOps.tempFile();
        SharedMemory sm1 = new SharedMemory(file);
        SharedMemory sm2 = new SharedMemory(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm2.write(ByteBuffers.fromString("bbb"));
        sm1.read();
        sm1.compact();
        Assert.assertEquals(sm2.read(), ByteBuffers.fromString("bbb"));
        sm2.write(ByteBuffers.fromString("cc"));
        sm2.compact();
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("cc"));
    }

    @Test
    public void testCompactionAcrossInstancesForWrites() {
        String file = FileOps.tempFile();
        SharedMemory sm1 = new SharedMemory(file);
        SharedMemory sm2 = new SharedMemory(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm1.write(ByteBuffers.fromString("bbb"));
        sm1.write(ByteBuffers.fromString("ccc"));
        sm1.read();
        sm2.read();
        sm1.compact();
        sm2.write(ByteBuffers.fromString("dddd"));
        sm1.write(ByteBuffers.fromString("ee"));
        Assert.assertEquals(sm2.read(), ByteBuffers.fromString("ccc"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("dddd"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("ee"));
    }

    @Test
    public void testCompactionAcrossInstancesForReads() {
        String file = FileOps.tempFile();
        SharedMemory sm1 = new SharedMemory(file);
        SharedMemory sm2 = new SharedMemory(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm1.write(ByteBuffers.fromString("bbb"));
        sm1.write(ByteBuffers.fromString("ccc"));
        sm1.read();
        sm2.compact();
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("bbb"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("ccc"));

    }

    @Test
    public void testMultipleConcurrentWriters() {
        SharedMemory memory = new SharedMemory();
        int writers = Random.getScaleCount();
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean passed = new AtomicBoolean(true);
        AtomicInteger ran = new AtomicInteger(0);
        for (int i = 0; i < writers; ++i) {
            executor.execute(() -> {
                try {
                    ByteBuffer data = ByteBuffer.allocate(4);
                    data.putInt(Random.getInt());
                    data.flip();
                    memory.write(data);
                    ran.incrementAndGet();
                }
                catch (OverlappingFileLockException e) {
                    passed.set(false);
                }
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        Assert.assertTrue(passed.get());
        Assert.assertEquals(ran.get(), writers);
    }

    @Test
    public void testCompactionRunsInBackground()
            throws InterruptedException, IOException {
        int frequency = SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS;
        SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = 50;
        try {
            String file = FileOps.tempFile();
            SharedMemory sm = new SharedMemory(file);
            long size = Files.size(Paths.get(file));
            sm.write(ByteBuffers.fromString("aaa"));
            sm.write(ByteBuffers.fromString("bbb"));
            sm.read();
            Thread.sleep(50 + 1);
            long lastCompaction = Reflection.get("lastCompaction", sm);
            sm.write(ByteBuffers.fromString("ccc"));
            while (Reflection.get("lastCompaction", sm)
                    .equals(lastCompaction)) {
                continue; // wait for compaction
            }
            Assert.assertTrue(size > Files.size(Paths.get(file)));
            SharedMemory sm2 = new SharedMemory(file);
            Assert.assertEquals(ByteBuffers.fromString("bbb"), sm2.read());
            Assert.assertEquals(ByteBuffers.fromString("ccc"), sm.read());
        }
        finally {
            SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = frequency;
        }
    }

    @Test
    public void testWriteReadAfterCompactionWhenNoUnreadMessages() {
        String file = FileOps.tempFile();
        SharedMemory sm1 = new SharedMemory(file);
        SharedMemory sm2 = new SharedMemory(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm2.read();
        sm1.write(ByteBuffers.fromString("bbb"));
        sm2.read();
        sm1.write(ByteBuffers.fromString("ccc"));
        sm2.read();
        sm2.compact();
        sm1.write(ByteBuffers.fromString("ddd"));
        Assert.assertEquals(ByteBuffers.fromString("ddd"), sm2.read());
    }

    @Test
    public void testReadWriteNoRaceCondition() throws InterruptedException {
        String file = FileOps.tempFile();
        SharedMemory sm1 = new SharedMemory(file);
        SharedMemory sm2 = new SharedMemory(file);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean read = new AtomicBoolean(false);
        Thread t1 = new Thread(() -> {
            sm1.read();
            read.set(true);
            latch.countDown();
        });
        Thread t2 = new Thread(() -> {
            sm2.write(ByteBuffers.fromString("aaa"));
            latch.countDown();
        });
        t2.start();
        t1.start();
        latch.await();
        Assert.assertTrue(read.get());
    }
    
}
