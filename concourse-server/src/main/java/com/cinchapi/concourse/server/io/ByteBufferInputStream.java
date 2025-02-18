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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link InputStream} that is backed by a {@link ByteBuffer}
 *
 * @author Jeff Nelson
 */
public class ByteBufferInputStream extends InputStream {

    /**
     * The backing buffer.
     */
    private final ByteBuffer buffer;

    /**
     * Construct a new instance.
     * 
     * @param buffer
     */
    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        return buffer.get();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if(len == 0) {
            return 0;
        }
        else {
            int count = Math.min(len, buffer.remaining());
            if(count == 0) {
                return -1;
            }
            else {
                buffer.get(b, off, count);
            }
            return count;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        long skippable = Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + (int) skippable);
        return skippable;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public synchronized void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        buffer.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

}
