/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.lib.offheap.memory.OffHeapMemory;

/**
 * A {@link ByteSink} is a destination to which bytes can be written, such as a
 * file.
 *
 * @author Jeff Nelson
 */
public interface ByteSink {

    /**
     * Return a {@link ByteSink} that passes through to a {@code byte[]}.
     * 
     * @param bytes
     * @return the {@link ByteSink}
     */
    public static ByteSink to(byte[] bytes) {
        return new ByteArraySink(bytes);
    }

    /**
     * Return a {@link ByteSink} that passes through to a {@link ByteBuffer}.
     * 
     * @param buffer
     * @return the {@link ByteSink}
     */
    public static ByteSink to(ByteBuffer buffer) {
        return new ByteBufferSink(buffer);
    }

    /**
     * Return a {@link ByteSink} that passes through to a {@link FileChannel}.
     * 
     * @param channel
     * @return the {@link ByteSink}
     */
    public static ByteSink to(FileChannel channel) {
        return new FileChannelSink(channel);
    }

    /**
     * Return a {@link ByteSink} that passes through to a {@link FileChannel}
     * using an in-memory buffer of {@code bufferSize}.
     * 
     * @param channel
     * @param bufferSize
     * @return the {@link ByteSink}
     */
    public static ByteSink to(FileChannel channel, int bufferSize) {
        return new FileChannelSink(channel, bufferSize);
    }

    /**
     * Return a {@link ByteSink} that passes through to {@link OffHeapMemory}.
     * 
     * @param memory
     * @return the {@link ByteSink}
     */
    public static ByteSink to(OffHeapMemory memory) {
        return new OffHeapMemoryByteSink(memory);
    }

    /**
     * Return a {@link ByteSink} that discards bytes that are written.
     * 
     * @return the {@link ByteSink}
     */
    public static ByteSink toDevNull() {
        return new NullByteSink();
    }

    /**
     * The name of the Charset to use for encoding/decoding. We use the name
     * instead of the charset object because Java caches encoders when
     * referencing them by name, but creates a new encorder object when
     * referencing them by Charset object.
     */
    public static final String UTF_8_CHARSET = StandardCharsets.UTF_8.name();

    /**
     * If any buffering is done, flush all bytes through the sink.
     */
    public default void flush() {}

    /**
     * Return the sink's current position.
     * 
     * @return the position of this sink
     */
    public long position();

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink put(byte value);

    /**
     * Put the {@code src} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param src
     * @return this
     */
    public ByteSink put(byte[] src);

    /**
     * Put the {@code src} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param src
     * @return this
     */
    public ByteSink put(ByteBuffer src);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putChar(char value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putDouble(double value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putFloat(float value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putInt(int value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putLong(long value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public ByteSink putShort(short value);

    /**
     * Put the {@code value} in this {@link ByteSink sink}, starting at the
     * current position.
     * 
     * @param value
     * @return this
     */
    public default ByteSink putUtf8(String value) {
        try {
            byte[] bytes = value.getBytes(UTF_8_CHARSET);
            put(bytes);
            return this;
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

}
