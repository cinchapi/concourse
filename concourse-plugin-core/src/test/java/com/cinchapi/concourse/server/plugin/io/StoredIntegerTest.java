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
package com.cinchapi.concourse.server.plugin.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.io.StoredInteger;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.google.common.base.Throwables;

/**
 * Unit tests for {@link StoredInteger}.
 * 
 * @author Jeff Nelson
 */
public class StoredIntegerTest {

    /**
     * Convenience method to create and position a {@link ByteBuffer} that
     * contains an integer {@code value}.
     * 
     * @param value the value to encode within the buffer
     * @return the buffer
     */
    private static ByteBuffer getByteBuffer(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        buffer.flip();
        return buffer;
    }

    private static StoredInteger getRandomStoredInteger() {
        String file = FileOps.tempFile();
        int value = Random.getInt();
        try (FileChannel channel = FileChannel.open(new File(file).toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            int before = Random.getScaleCount();
            int after = Random.getScaleCount();
            for (int i = 0; i < before; ++i) {
                channel.write(getByteBuffer(Random.getInt()));
            }
            channel.write(getByteBuffer(value));
            for (int i = 0; i < after; ++i) {
                channel.write(getByteBuffer(Random.getInt()));
            }
            int position = 4 * before;
            StoredInteger integer = new StoredInteger(file, position);
            return integer;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testGetBeginning() {
        try {
            String file = FileOps.tempFile();
            int value = Random.getInt();
            FileChannel channel = FileChannel.open(new File(file).toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            channel.write(getByteBuffer(value));
            for (int i = 0; i < Random.getScaleCount(); ++i) {
                channel.write(getByteBuffer(Random.getInt()));
            }
            StoredInteger integer = new StoredInteger(channel);
            for (int i = 0; i < Random.getScaleCount(); ++i) {
                Assert.assertEquals(value, integer.get());
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testGetRandomPosition() {
        try {
            String file = FileOps.tempFile();
            int value = Random.getInt();
            FileChannel channel = FileChannel.open(new File(file).toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            int before = Random.getScaleCount();
            int after = Random.getScaleCount();
            for (int i = 0; i < before; ++i) {
                channel.write(getByteBuffer(Random.getInt()));
            }
            channel.write(getByteBuffer(value));
            for (int i = 0; i < after; ++i) {
                channel.write(getByteBuffer(Random.getInt()));
            }
            int position = 4 * before;
            StoredInteger integer = new StoredInteger(file, position);
            for (int i = 0; i < Random.getScaleCount(); ++i) {
                Assert.assertEquals(value, integer.get());
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testSet() {
        StoredInteger value = getRandomStoredInteger();
        int newValue = 0;
        while (newValue == 0 || newValue == value.get()) {
            newValue = Random.getInt();
        }
        value.set(newValue);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            Assert.assertEquals(newValue, value.get());
        }
    }

    @Test
    public void testAddAndGet() {
        StoredInteger value = getRandomStoredInteger();
        int add = Random.getInt();
        int expected = value.get() + add;
        Assert.assertEquals(expected, value.addAndGet(add));
    }
  
}
