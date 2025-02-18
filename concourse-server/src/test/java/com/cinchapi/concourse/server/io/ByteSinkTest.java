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
package com.cinchapi.concourse.server.io;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link ByteSink}.
 *
 * @author Jeff Nelson
 */
public class ByteSinkTest {

    @Test
    public void testNullByteSinkTracksPosition() {
        ByteSink a = ByteSink.to(ByteBuffer.allocate(100));
        ByteSink b = ByteSink.toDevNull();
        a.put((byte) 1);
        b.put((byte) 1);
        Assert.assertEquals(a.position(), b.position());
        a.putShort((short) 1);
        b.putShort((short) 1);
        Assert.assertEquals(a.position(), b.position());
        a.putInt(1);
        b.putInt(1);
        Assert.assertEquals(a.position(), b.position());
        a.putLong(1);
        b.putLong(1);
        Assert.assertEquals(a.position(), b.position());
        a.putFloat(1);
        b.putFloat(1);
        Assert.assertEquals(a.position(), b.position());
        a.putDouble(1);
        b.putDouble(1);
        Assert.assertEquals(a.position(), b.position());
        a.putUtf8("hello");
        b.putUtf8("hello");
        Assert.assertEquals(a.position(), b.position());
        a.put(ByteBuffer.allocate(10));
        b.put(ByteBuffer.allocate(10));
        Assert.assertEquals(a.position(), b.position());
        a.put(new byte[5]);
        b.put(new byte[5]);
        Assert.assertEquals(a.position(), b.position());
    }

    @Test
    public void testFileChannelSinkWithBufferTracksPosition() {
        FileChannel channel = FileSystem
                .getFileChannel(Paths.get(TestData.getTemporaryTestFile()));
        ByteSink sink = ByteSink.to(channel, 64);
        int count = TestData.getScaleCount();
        int expected = 0;
        for (int i = 0; i < count; ++i) {
            String str = Random.getString();
            expected += ByteBuffers.fromUtf8String(str).capacity();
            sink.putUtf8(str);
            Assert.assertEquals(expected, sink.position());
        }
    }

    @Test
    public void testFileChannelSinkWithBufferAccuracy() throws IOException {
        java.util.Random random = new java.util.Random();
        ByteBuffer expected = ByteBuffer
                .allocate(random.nextInt(64000000) + 1000000);
        ByteSink a = ByteSink.to(expected);
        FileChannel channel = FileSystem
                .getFileChannel(Paths.get(TestData.getTemporaryTestFile()));
        ByteSink b = ByteSink.to(channel, 512);
        while (expected.hasRemaining()) {
            int seed = Math.abs(random.nextInt());
            try {
                if(seed % 3 == 0) {
                    String src = Random.getString();
                    a.putUtf8(src);
                    b.putUtf8(src);
                }
                else if(seed % 5 == 0) {
                    long src = random.nextLong();
                    a.putLong(src);
                    b.putLong(src);
                }
                else if(seed % 4 == 0) {
                    int src = random.nextInt();
                    a.putInt(src);
                    b.putInt(src);
                }
                else if(seed % 2 == 0) {
                    double src = random.nextDouble();
                    a.putDouble(src);
                    b.putDouble(src);
                }
                else {
                    byte[] src = new byte[1024];
                    Arrays.fill(src, (byte) 17);
                    a.put(src);
                    b.put(src);
                }
            }
            catch (BufferOverflowException e) {
                break;
            }
        }
        a.flush();
        b.flush();
        expected.flip();
        ByteBuffer actual = ByteBuffer.allocate((int) channel.position());
        channel.position(0);
        channel.read(actual);
        actual.flip();
        Assert.assertEquals(expected.remaining(), actual.remaining());
        while (expected.hasRemaining()) {
            Assert.assertEquals(expected.get(), actual.get());
        }
    }

}
