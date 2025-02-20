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

import java.nio.ByteBuffer;

import com.cinchapi.lib.offheap.memory.OffHeapMemory;

/**
 * A {@link ByteSink} that writes to {@link OffHeapMemory}.
 *
 * @author Jeff Nelson
 */
final class OffHeapMemoryByteSink implements ByteSink {

    /**
     * The destination for the bytes.
     */
    private final OffHeapMemory memory;

    /**
     * Construct a new instance.
     * 
     * @param memory
     */
    public OffHeapMemoryByteSink(OffHeapMemory memory) {
        this.memory = memory;
    }

    @Override
    public long position() {
        return memory.position();
    }

    @Override
    public ByteSink put(byte value) {
        memory.put(value);
        return this;
    }

    @Override
    public ByteSink put(byte[] src) {
        for (int i = 0; i < src.length; ++i) {
            memory.put(src[i]);
        }
        return this;
    }

    @Override
    public ByteSink put(ByteBuffer src) {
        while (src.hasRemaining()) {
            memory.put(src.get());
        }
        return this;
    }

    @Override
    public ByteSink putChar(char value) {
        memory.putChar(value);
        return this;
    }

    @Override
    public ByteSink putDouble(double value) {
        memory.putDouble(value);
        return this;
    }

    @Override
    public ByteSink putFloat(float value) {
        memory.putFloat(value);
        return this;
    }

    @Override
    public ByteSink putInt(int value) {
        memory.putInt(value);
        return this;
    }

    @Override
    public ByteSink putLong(long value) {
        memory.putLong(value);
        return this;
    }

    @Override
    public ByteSink putShort(short value) {
        memory.putShort(value);
        return this;
    }

}