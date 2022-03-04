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

/**
 * A {@link ByteSink} that writes to a {@link ByteBuffer}.
 *
 * @author Jeff Nelson
 */
final class ByteBufferSink implements ByteSink {

    /**
     * The destination where bytes are written.
     */
    private final ByteBuffer buffer;

    /**
     * Construct a new instance.
     * 
     * @param buffer
     */
    ByteBufferSink(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public long position() {
        return buffer.position();
    }

    @Override
    public ByteSink put(byte value) {
        buffer.put(value);
        return this;
    }

    @Override
    public ByteSink put(byte[] src) {
        buffer.put(src);
        return this;
    }

    @Override
    public ByteSink put(ByteBuffer src) {
        buffer.put(src);
        return this;
    }

    @Override
    public ByteSink putChar(char value) {
        buffer.putChar(value);
        return this;
    }

    @Override
    public ByteSink putDouble(double value) {
        buffer.putDouble(value);
        return this;
    }

    @Override
    public ByteSink putFloat(float value) {
        buffer.putFloat(value);
        return this;
    }

    @Override
    public ByteSink putInt(int value) {
        buffer.putInt(value);
        return this;
    }

    @Override
    public ByteSink putLong(long value) {
        buffer.putLong(value);
        return this;
    }

    @Override
    public ByteSink putShort(short value) {
        buffer.putShort(value);
        return this;
    }

}
