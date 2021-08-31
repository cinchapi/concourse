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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.cinchapi.concourse.util.Integers;
import com.google.common.base.Preconditions;

/**
 * An {@link CloseableIterator} that streams bytes in a {@link Path file} under
 * the assumption that they represent a {@link ByteableCollections
 * ByteableCollection} and returns a series of {ByteBuffer ByteBuffers} that can
 * be used to deserialize a {@link Byteable} object.
 *
 * @author Jeff Nelson
 */
class ByteableCollectionStreamIterator implements
        CloseableIterator<ByteBuffer> {

    /**
     * Return a {@link ByteableCollectionStreamIterator} with a buffer size of
     * {@code bufferSize} for the bytes in {@code file}, start {@code position}
     * and running for {@code length}.
     * 
     * @param file
     * @param position
     * @param length
     * @param bufferSize
     * @return the {@link CloseableIterator}
     */
    public static ByteableCollectionStreamIterator from(Path file,
            long position, long length, int bufferSize) {
        return new ByteableCollectionStreamIterator(file, position, length,
                bufferSize);
    }

    /**
     * The bytes that have been {@link #read(int) read} and are currently used
     * to {@link #findNext() find} the {@link #next} element.
     */
    private ByteBuffer buffer;

    /**
     * The number of bytes to {@link #read(int) read} at a time.
     */
    private final int bufferSize;

    /**
     * The {@link FileChannel} used for {@link #read(int) reading}.
     */
    private final FileChannel channel;

    /**
     * The boundary after which no bytes should be read in {@link #channel}.
     */
    private final long limit;

    /**
     * The element that will be returned from {@link #next()} if it is not
     * {@code null}. If this value is {@code null} it signifies that the
     * {@link Iterator} has no further elements.
     */
    private ByteBuffer next;

    /**
     * The current position where data is mapped from {@link #channel}.
     * <p>
     * NOTE: This does not track the {@link ByteBuffer#position() position} of
     * the {@link #buffer}. It is used to determine where the next
     * {@link #read(int)} should begin.
     * </p>
     */
    private long position;

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param position
     * @param length
     * @param bufferSize
     */
    private ByteableCollectionStreamIterator(Path file, long position,
            long length, int bufferSize) {
        // Preconditions.checkArgument(length >= 4);
        Preconditions.checkArgument(bufferSize >= 0);
        this.channel = FileSystem.getFileChannel(file);
        this.position = position;
        this.limit = this.position + length;
        this.bufferSize = length < Integer.MAX_VALUE && length > bufferSize
                ? Integers.nextPowerOfTwo(bufferSize)
                : (int) length;
        read(bufferSize); // instantiates #buffer
    }

    @Override
    public void close() throws IOException {
        FileSystem.closeFileChannel(channel);
    }

    @Override
    public boolean hasNext() {
        findNext();
        return next != null;
    }

    @Override
    public ByteBuffer next() {
        if(next == null) {
            // In case there wasn't a prior call to #hasNext()
            findNext();
            if(next == null) {
                throw new NoSuchElementException();
            }
        }
        return next;
    }

    @Override
    protected void finalize() throws Throwable {
        this.closeQuietly();
    }

    /**
     * Set the {@link #next} element and update the {@link #position}
     * accordingly. If necessary, the contents of the {@link #buffer} may change
     * to fully read {@link #next} into memory.
     */
    private void findNext() {
        long at = position - buffer.remaining();
        if(at >= limit) {
            next = null;
            return;
        }
        if(buffer.remaining() <= 4) {
            // There aren't enough bytes in the buffer to read the next size
            // and element, so refresh the buffer's content.
            position -= buffer.remaining();
            read(bufferSize);
        }
        int size = buffer.getInt();
        if(size <= 0) {
            next = null;
            return;
        }
        if(size > buffer.remaining()) {
            // There aren't enough bytes in the buffer (e.g. the size of the
            // next item > bufferSize), so backtrack and (re)read enough bytes,
            // but try to keep the buffer sized as a power of two to maximize
            // I/O efficiency
            position -= buffer.remaining();
            read(size);
        }
        next = buffer.slice();
        next.limit(size);
        buffer.position(buffer.position() + size);
    }

    /**
     * Read {@code size} bytes into {@code buffer} from {@code position} in
     * {@code channel} and increment {@link #position} by the same.
     * 
     * @param size the number of bytes to read
     */
    private void read(int size) {
        try {
            buffer = channel.map(MapMode.READ_ONLY, position, size);
            position += buffer.capacity();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

}
