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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;

import com.cinchapi.common.io.ByteBuffers;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

/**
 * A {@link ByteSink} that writes to a byte[].
 * <p>
 * NOTE: Bytes are written in {@link java.nio.ByteOrder#BIG_ENDIAN big endian}
 * byte order.
 * </p>
 *
 * @author Jeff Nelson
 */
final class ByteArraySink implements ByteSink {

    /**
     * The destination byte[].
     */
    private final byte[] bytes;

    /**
     * The current position for writing
     */
    private int position = 0;

    /**
     * Construct a new instance.
     * 
     * @param bytes
     */
    ByteArraySink(byte[] bytes) {
        this.bytes = bytes;
        this.position = 0;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public ByteSink put(byte value) {
        bytes[position++] = value;
        return this;
    }

    @Override
    public ByteSink put(byte[] src) {
        System.arraycopy(src, 0, bytes, position, src.length);
        position += src.length;
        return this;
    }

    @Override
    public ByteSink put(ByteBuffer src) {
        return put(ByteBuffers.getByteArray(src));
    }

    @Override
    public ByteSink putChar(char value) {
        return put(Chars.toByteArray(value));
    }

    @Override
    public ByteSink putDouble(double value) {
        return putLong(Double.doubleToRawLongBits(value));
    }

    @Override
    public ByteSink putFloat(float value) {
        return putInt(Float.floatToRawIntBits(value));
    }

    @Override
    public ByteSink putInt(int value) {
        return put(Ints.toByteArray(value));
    }

    @Override
    public ByteSink putLong(long value) {
        return put(Longs.toByteArray(value));
    }

    @Override
    public ByteSink putShort(short value) {
        return put(Shorts.toByteArray(value));
    }

}
