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
package com.cinchapi.concourse.server.plugin.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.RandomStringGenerator;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link SharedMemory}.
 * 
 * @author Jeff Nelson
 */
public class SharedMemoryTest extends InterProcessCommunicationTest {

    @Override
    protected InterProcessCommunication getInterProcessCommunication() {
        return new SharedMemory();
    }

    @Override
    protected InterProcessCommunication getInterProcessCommunication(
            String file) {
        return new SharedMemory(file);
    }

    @Override
    protected InterProcessCommunication getInterProcessCommunication(
            String file, int capacity) {
        return new SharedMemory(file, capacity);
    }

    @Test
    public void testCompactionRunsInBackground()
            throws InterruptedException, IOException {
        int frequency = SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS;
        SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = 50;
        try {
            String file = FileOps.tempFile();
            InterProcessCommunication sm = getInterProcessCommunication(file);
            long size = Files.size(Paths.get(file));
            sm.write(ByteBuffers.fromUtf8String("aaa"));
            sm.write(ByteBuffers.fromUtf8String("bbb"));
            sm.read();
            Thread.sleep(50 + 1);
            long lastCompaction = Reflection.get("lastCompaction", sm);
            sm.write(ByteBuffers.fromUtf8String("ccc"));
            while (Reflection.get("lastCompaction", sm)
                    .equals(lastCompaction)) {
                continue; // wait for compaction
            }
            Assert.assertTrue(size > Files.size(Paths.get(file)));
            InterProcessCommunication sm2 = getInterProcessCommunication(file);
            Assert.assertEquals(ByteBuffers.fromUtf8String("bbb"), sm2.read());
            Assert.assertEquals(ByteBuffers.fromUtf8String("ccc"), sm.read());
        }
        finally {
            SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = frequency;
        }
    }

    @Test
    public void testCompactionByReaderWontRuinWriter()
            throws InterruptedException, IOException { // bug repro
        int original = SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS;
        SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = 100;
        try {
            String file = FileOps.tempFile();
            InterProcessCommunication writer = getInterProcessCommunication(
                    file, 4);
            InterProcessCommunication reader = getInterProcessCommunication(
                    file, 4);
            ByteBuffer message = ByteBuffers.fromUtf8String(
                    new RandomStringGenerator().nextString(15000));
            writer.write(message);
            message.flip();
            Thread.sleep(SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS + 10); // ensure
                                                                            // compaction
                                                                            // starts
            reader.read();
            writer.write(message);
            message.flip();
            ByteBuffer actual = reader.read();
            Assert.assertEquals(message, actual);
        }
        finally {
            SharedMemory.COMPACTION_FREQUENCY_IN_MILLIS = original;
        }
    }

    @Test
    public void testCompaction() {
        int toRead = Random.getScaleCount();
        int total = toRead + Random.getScaleCount();
        InterProcessCommunication memory = getInterProcessCommunication();
        List<String> expected = Lists.newArrayList();
        for (int i = 0; i < total; ++i) {
            String message = Random.getString();
            memory.write(ByteBuffers.fromUtf8String(message));
            expected.add(message);
        }
        List<String> actual = Lists
                .newArrayListWithExpectedSize(expected.size());
        for (int i = 0; i < toRead; ++i) {
            String message = ByteBuffers.getUtf8String(memory.read());
            actual.add(message);
        }
        int pos0 = ((MappedAtomicInteger) Reflection.get("nextRead", memory))
                .get();
        memory.compact();
        int pos1 = ((MappedAtomicInteger) Reflection.get("nextRead", memory))
                .get();
        Assert.assertTrue(pos0 > pos1);
        for (int i = toRead; i < total; ++i) {
            String message = ByteBuffers.getUtf8String(memory.read());
            actual.add(message);
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCompactionDecreasesUnderlyingFileSize() {
        String file = FileOps.tempFile();
        InterProcessCommunication memory = getInterProcessCommunication(file);
        try {
            memory.write(ByteBuffers.fromUtf8String("hello world"));
            long size = Files.size(Paths.get(file));
            memory.read();
            memory.compact();
            Assert.assertTrue(size > Files.size(Paths.get(file)));
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    @Test
    public void testCompactionIsReflectedAcrossInstances() {
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file);
        InterProcessCommunication sm2 = getInterProcessCommunication(file);
        sm1.write(ByteBuffers.fromUtf8String("aaa"));
        sm2.write(ByteBuffers.fromUtf8String("bbb"));
        sm1.read();
        sm1.compact();
        Assert.assertEquals(sm2.read(), ByteBuffers.fromUtf8String("bbb"));
        sm2.write(ByteBuffers.fromUtf8String("cc"));
        sm2.compact();
        Assert.assertEquals(sm1.read(), ByteBuffers.fromUtf8String("cc"));
    }

    @Test
    public void testCompactionAcrossInstancesForWrites() {
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file);
        InterProcessCommunication sm2 = getInterProcessCommunication(file);
        sm1.write(ByteBuffers.fromUtf8String("aaa"));
        sm1.write(ByteBuffers.fromUtf8String("bbb"));
        sm1.write(ByteBuffers.fromUtf8String("ccc"));
        sm1.read();
        sm2.read();
        sm1.compact();
        sm2.write(ByteBuffers.fromUtf8String("dddd"));
        sm1.write(ByteBuffers.fromUtf8String("ee"));
        Assert.assertEquals(sm2.read(), ByteBuffers.fromUtf8String("ccc"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromUtf8String("dddd"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromUtf8String("ee"));
    }

}
