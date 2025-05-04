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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnull;

import com.cinchapi.common.base.CheckedExceptions;
import com.google.common.base.Preconditions;

/**
 * A {@link ByteSink} that writes to a {@link FileChannel}.
 *
 * @author Jeff Nelson
 */
final class FileChannelSink implements ByteSink {

    /**
     * The default value for {@link #bufferSize}.
     */
    private static final int DEFAULT_BUFFER_SIZE = 65536;

    /**
     * Internal buffer used to batch write data to the {@code channel}.
     */
    @Nonnull
    private ByteBuffer buffer;

    /**
     * The {@code buffer} size.
     */
    private final int bufferSize;

    /**
     * The destination where bytes are written.
     */
    private final FileChannel channel;

    /**
     * Construct a new instance.
     * 
     * @param channel
     */
    FileChannelSink(FileChannel channel) {
        this(channel, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Construct a new instance.
     * 
     * @param channel
     */
    FileChannelSink(FileChannel channel, int bufferSize) {
        Preconditions.checkArgument(bufferSize >= 0, "Negative buffer size");
        this.channel = channel;
        this.bufferSize = bufferSize;
        this.buffer = allocateBuffer();
    }

    @Override
    public void flush() {
        buffer.flip();
        while (buffer.hasRemaining()) {
            try {
                channel.write(buffer);
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        buffer = allocateBuffer();
    }

    @Override
    public long position() {
        try {
            return channel.position() + buffer.position();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }

    }

    @Override
    public ByteSink put(byte value) {
        int need = 1;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.put(value);
        }
        return this;
    }

    @Override
    public ByteSink put(byte[] src) {
        int need = src.length;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.wrap(src);
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.put(src);
        }
        return this;
    }

    @Override
    public ByteSink put(ByteBuffer src) {
        int need = src.remaining();
        if(need > bufferSize) {
            flush();
            write(src);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.put(src);
        }
        return this;
    }

    @Override
    public ByteSink putChar(char value) {
        int need = 2;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putChar(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putChar(value);
        }
        return this;
    }

    @Override
    public ByteSink putDouble(double value) {
        int need = 8;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putDouble(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putDouble(value);
        }
        return this;
    }

    @Override
    public ByteSink putFloat(float value) {
        int need = 4;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putFloat(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putFloat(value);
        }
        return this;
    }

    @Override
    public ByteSink putInt(int value) {
        int need = 4;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putInt(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putInt(value);
        }
        return this;
    }

    @Override
    public ByteSink putLong(long value) {
        int need = 8;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putLong(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putLong(value);
        }
        return this;
    }

    @Override
    public ByteSink putShort(short value) {
        int need = 2;
        if(need > bufferSize) {
            flush();
            ByteBuffer buffer = ByteBuffer.allocate(need);
            buffer.putShort(value);
            buffer.flip();
            write(buffer);
        }
        else {
            while (need > buffer.remaining()) {
                flush();
            }
            buffer.putShort(value);
        }
        return this;
    }

    /**
     * Allocate a new {@link ByteBuffer}.
     * 
     * @return the allocated buffer
     */
    private ByteBuffer allocateBuffer() {
        return ByteBuffer.allocate(bufferSize);
    }

    /**
     * Write the {@code src} to the {@link #channel}.
     * 
     * @param src
     */
    private void write(ByteBuffer src) {
        while (src.hasRemaining()) {
            try {
                channel.write(src);
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
    }

}