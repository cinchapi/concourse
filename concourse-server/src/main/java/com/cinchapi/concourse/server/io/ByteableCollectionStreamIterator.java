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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.google.common.base.Preconditions;

/**
 * An {@link CloseableIterator} that streams bytes in a {@link Path file} under
 * the assumption that they represent a {@link ByteableCollections
 * ByteableCollection} and returns a series of {ByteBuffer ByteBuffers} that can
 * be used to deserialize a {@link Byteable} object.
 * 
 * <p>
 * <strong>Warning:</strong> {@link ByteBuffer ByteBuffers} that are returned
 * from {@link #next()} should <strong>not</strong> be stored in memory or
 * assumed to be long-lived (e.g. each call to {@link #next()} may invalidate or
 * change the state of the previously returned {@link ByteBuffer}. If streamed
 * {@link ByteBuffer ByteBuffers} need to be accessed after processing, make a
 * copy of the value returned from {@link #next()}.
 * </p>
 *
 * @author Jeff Nelson
 */
class ByteableCollectionStreamIterator implements
        CloseableIterator<ByteBuffer> {

    /**
     * Return a {@link ByteableCollectionStreamIterator} with a buffer size of
     * {@code bufferSize} for the bytes in {@code channel}, start
     * {@code position} and running for {@code length}.
     * 
     * @param channel
     * @param position
     * @param length
     * @param bufferSize
     * @return the {@link CloseableIterator}
     */
    public static ByteableCollectionStreamIterator from(FileChannel channel,
            long position, long length, int bufferSize) {
        return new ByteableCollectionStreamIterator(channel, position, length,
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
     * A flag that is set to {@code true} when {@link #findNext()} is run and
     * set to {@code false} after the {@link #next() next} element is consumed.
     * This ensure that multiple calls to {@link #hasNext()} don't advance the
     * iterator unless a call to {@link #next()} is made.
     */
    private boolean found = false;

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
     * A {@link ByteBuffer#duplicate() duplicate} of the current {@link #buffer}
     * that is {@link ByteBuffer#slice() sliced}, {@link ByteBuffer#limit(int)
     * limited} and returned from {@link #next()} in an effort to prevent the
     * creation of temporary objects during the course of the stream.
     */
    private ByteBuffer slice;

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param position
     * @param length
     * @param bufferSize
     */
    private ByteableCollectionStreamIterator(FileChannel channel, long position,
            long length, int bufferSize) {
        Preconditions.checkArgument(bufferSize >= 1);
        if(length <= 0) {
            this.channel = null;
            this.position = 0;
            this.limit = 0;
            this.bufferSize = 0;
            this.buffer = ByteBuffer.allocate(0);
            this.next = null;
        }
        else {
            this.channel = channel;
            this.position = position;
            this.limit = this.position + length;
            this.bufferSize = Math.min(bufferSize,
                    length > Integer.MAX_VALUE ? Integer.MAX_VALUE
                            : (int) length);
            read(bufferSize); // instantiates #buffer
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public boolean hasNext() {
        findNext();
        return next != null;
    }

    @Override
    public ByteBuffer next() {
        findNext();
        found = false;
        if(next == null) {
            throw new NoSuchElementException();
        }
        return next;
    }

    /**
     * Set the {@link #next} element and update the {@link #position}
     * accordingly. If necessary, the contents of the {@link #buffer} may change
     * to fully read {@link #next} into memory.
     */
    private void findNext() {
        if(!found) {
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
                // next item > bufferSize), so backtrack and (re)read enough
                // bytes, but try to keep the buffer sized as a power of two to
                // maximize I/O efficiency
                position -= buffer.remaining();
                read(size);
            }
            slice.rewind();
            slice.limit(slice.capacity());
            slice.position(buffer.position());
            slice.limit(buffer.position() + size);
            next = slice;
            buffer.position(buffer.position() + size);
            found = true;
        }
    }

    /**
     * Read {@code size} bytes into {@code buffer} from {@code position} in
     * {@code channel} and increment {@link #position} by the same.
     * 
     * @param size the number of bytes to read
     */
    private void read(int size) {
        try {
            buffer = ByteBuffer.allocate(size);
            while (buffer.hasRemaining()
                    && channel.read(buffer, position) >= 0) {
                position += buffer.position();
            }
            buffer.flip();
            slice = buffer.duplicate();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

}
