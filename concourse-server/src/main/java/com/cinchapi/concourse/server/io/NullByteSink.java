/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
 * A {@link ByteSink} that does write to anything, but keeps track of how many
 * bytes have been dumped.
 *
 * @author Jeff Nelson
 */
final class NullByteSink implements ByteSink {

    /**
     * The tracked "position".
     */
    private int position;

    @Override
    public long position() {
        return position;
    }

    @Override
    public ByteSink put(byte value) {
        ++position;
        return this;
    }

    @Override
    public ByteSink put(byte[] src) {
        position += src.length;
        return this;
    }

    @Override
    public ByteSink put(ByteBuffer src) {
        position += src.remaining();
        return this;
    }

    @Override
    public ByteSink putChar(char value) {
        position += 1;
        return this;
    }

    @Override
    public ByteSink putDouble(double value) {
        position += 8;
        return this;
    }

    @Override
    public ByteSink putFloat(float value) {
        position += 4;
        return this;
    }

    @Override
    public ByteSink putInt(int value) {
        position += 4;
        return this;
    }

    @Override
    public ByteSink putLong(long value) {
        position += 8;
        return this;
    }

    @Override
    public ByteSink putShort(short value) {
        position += 2;
        return this;
    }

}
