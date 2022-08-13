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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.google.common.base.Preconditions;

/**
 * This class contains utilities for encoding collections of {@link Byteable}
 * objects into a byte array and iterating over the sequences of bytes that
 * represent those objects.
 * 
 * @author Jeff Nelson
 */
public class ByteableCollections {

    /**
     * Return an iterator that will traverse {@code bytes} and return a series
     * of byte buffers, each of which can be used to reconstruct a
     * {@link Byteable} object that is a member of a collection.
     * 
     * @param bytes
     * @return the iterator
     */
    public static Iterator<ByteBuffer> iterator(ByteBuffer bytes) {
        return new ByteableCollectionIterator(bytes);
    }

    /**
     * Return an iterator that will traverse {@code bytes} and return a series
     * of fixed size byte buffers, each of which can be used to reconstruct a
     * {@link Byteable} object that is a member of a collection.
     * 
     * @param bytes
     * @return the iterator
     */
    public static Iterator<ByteBuffer> iterator(ByteBuffer bytes,
            int sizePerElement) {
        return new FixedSizeByteableCollectionIterator(bytes);
    }

    /**
     * Return an {@link Iterator} that will traverse the bytes in
     * {@code channel} and return a series of {@link ByteBuffer byte buffers},
     * each of which can be used to reconstruct a {@link Byteable} object.
     * Unlike the {@link #iterator(ByteBuffer)} method, this one only reads
     * {@link bufferSize} bytes from disk at a time, which is necessary when its
     * infeasible to read the entire file into memory at once.
     * 
     * <p>
     * <strong>Warning:</strong> {@link ByteBuffer ByteBuffers} that are
     * returned from {@link #next()} should <strong>not</strong> be stored in
     * memory or assumed to be long-lived (e.g. each call to {@link #next()} may
     * invalidate or change the state of the previously returned
     * {@link ByteBuffer}. If streamed {@link ByteBuffer ByteBuffers} need to be
     * accessed after processing, make a copy of the value returned from
     * {@link #next()}.
     * </p>
     * 
     * @param channel
     * @param position the channel index from which to start reading
     * @param length
     * @param bufferSize
     * @return the {@link Iterator}
     */
    public static CloseableIterator<ByteBuffer> stream(FileChannel channel,
            long position, long length, int bufferSize) {
        return ByteableCollectionStreamIterator.from(channel, position, length,
                bufferSize);
    }

    /**
     * Return an {@link Iterator} that will traverse the bytes in {@code file}
     * and return a series of {@link ByteBuffer byte buffers}, each of which can
     * be used to reconstruct a {@link Byteable} object. Unlike the
     * {@link #iterator(ByteBuffer)} method, this one only reads
     * {@link bufferSize} bytes from disk at a time, which is necessary when its
     * infeasible to read the entire file into memory at once.
     * 
     * <p>
     * <strong>Warning:</strong> {@link ByteBuffer ByteBuffers} that are
     * returned from {@link #next()} should <strong>not</strong> be stored in
     * memory or assumed to be long-lived (e.g. each call to {@link #next()} may
     * invalidate or change the state of the previously returned
     * {@link ByteBuffer}. If streamed {@link ByteBuffer ByteBuffers} need to be
     * accessed after processing, make a copy of the value returned from
     * {@link #next()}.
     * </p>
     * 
     * @param file
     * @param bufferSize
     * @return the {@link Iterator}
     */
    public static CloseableIterator<ByteBuffer> stream(Path file,
            int bufferSize) {
        return ByteableCollectionStreamIterator.from(
                FileSystem.getFileChannel(file), 0,
                FileSystem.getFileSize(file.toString()), bufferSize);
    }

    /**
     * Return an {@link Iterator} that will traverse the bytes in {@code file}
     * and return a series of {@link ByteBuffer byte buffers}, each of which can
     * be used to reconstruct a {@link Byteable} object. Unlike the
     * {@link #iterator(ByteBuffer)} method, this one only reads
     * {@link bufferSize} bytes from disk at a time, which is necessary when its
     * infeasible to read the entire file into memory at once.
     * 
     * <p>
     * <strong>Warning:</strong> {@link ByteBuffer ByteBuffers} that are
     * returned from {@link #next()} should <strong>not</strong> be stored in
     * memory or assumed to be long-lived (e.g. each call to {@link #next()} may
     * invalidate or change the state of the previously returned
     * {@link ByteBuffer}. If streamed {@link ByteBuffer ByteBuffers} need to be
     * accessed after processing, make a copy of the value returned from
     * {@link #next()}.
     * </p>
     * 
     * @param file
     * @param position the file index from which to start reading
     * @param length
     * @param bufferSize
     * @return the {@link Iterator}
     */
    public static CloseableIterator<ByteBuffer> stream(Path file, long position,
            long length, int bufferSize) {
        return ByteableCollectionStreamIterator.from(
                FileSystem.getFileChannel(file), position, length, bufferSize);
    }

    /**
     * Return an {@link Iterator} that will traverse the bytes in {@code file}
     * and return a series of {@link ByteBuffer byte buffers}, each of which can
     * be used to reconstruct a {@link Byteable} object. Unlike the
     * {@link #iterator(ByteBuffer)} method, this one only reads
     * {@link bufferSize} bytes from disk at a time, which is necessary when its
     * infeasible to read the entire file into memory at once.
     * 
     * @param file
     * @param position the file index from which to start reading
     * @param length the total number of bytes to read before determining that
     *            there are no more elements
     * @param bufferSize - must be large enough to accommodate the largest
     *            element that will be returned by the iterator
     * @return the iterator
     * @deprecated use {@link #stream(Path, long, long, int)} instead
     */
    @Deprecated
    public static Iterator<ByteBuffer> streamingIterator(Path file,
            long position, long length, int bufferSize) {
        return new Iterator<ByteBuffer>() {

            private long bufSize = bufferSize;
            private boolean expandBuffer = false;
            private long index = position;
            private Iterator<ByteBuffer> it = null;
            private long limit = index + length;

            {

                adjustBuffer();
            }

            @Override
            public boolean hasNext() {
                ByteBuffer backingBytes = ((ByteableCollectionIterator) it).bytes;
                if(index < limit && it.hasNext()) {
                    return true;
                }
                else if(index < limit && limit - index >= 4) {
                    if(backingBytes.remaining() >= 4) {
                        // In order to know if we've reached a state where the
                        // remaining bytes in the file are null, we need to peek
                        // at at least an int. If there are less than 4 bytes
                        // left in the buffer, just assume we need to adjust the
                        // buffer and try again
                        if(backingBytes.getInt() == 0) {
                            backingBytes.position(backingBytes.position() - 4);
                            return false;
                        }
                        else {
                            backingBytes.position(backingBytes.position() - 4);
                        }
                    }
                    adjustBuffer();
                    expandBuffer = true;
                    return hasNext();
                }
                else {
                    return false;
                }
            }

            @Override
            public ByteBuffer next() {
                try {
                    ByteBuffer next = it.next();
                    index += next.capacity() + 4;
                    expandBuffer = false;
                    return next;
                }
                catch (BufferUnderflowException e) {
                    adjustBuffer();
                    return next();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            /**
             * Fill the {@link #limbo} with the smaller of the remaining bytes
             * in the file of {@code bufferSize} bytes from the current
             * {@code position}.
             */
            private void adjustBuffer() {
                if(expandBuffer) {
                    bufSize *= 2;
                }
                it = iterator(FileSystem.map(file, MapMode.READ_ONLY, index,
                        Math.min(limit - index, bufSize)));
            }

        };
    }

    /**
     * Return an {@link Iterator} that will traverse the bytes in {@code file}
     * and return a series of {@link ByteBuffer byte buffers}, each of which can
     * be used to reconstruct a {@link Byteable} object. Unlike the
     * {@link #iterator(ByteBuffer)} method, this one only reads
     * {@link bufferSize} bytes from disk at a time, which is necessary when its
     * infeasible to read the entire file into memory at once.
     * 
     * @param file
     * @param bufferSize - must be large enough to accommodate the largest
     *            element that will be returned by the iterator
     * @return the iterator
     * @deprecated use {@link #stream(Path, int)} instead
     */
    @Deprecated
    public static Iterator<ByteBuffer> streamingIterator(final String file,
            final int bufferSize) {
        return streamingIterator(Paths.get(file), 0,
                FileSystem.getFileSize(file), bufferSize);
    }

    /**
     * Encode the collection as a sequence of bytes.
     * 
     * @param collection
     * @return a byte array
     */
    public static ByteBuffer toByteBuffer(
            Collection<? extends Byteable> collection) {
        int size = 0;
        for (Byteable object : collection) {
            size += (object.size() + 4);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (Byteable object : collection) {
            buffer.putInt(object.size());
            object.copyTo(buffer);
        }
        buffer.rewind();
        return buffer;
    }

    /**
     * Encode the collection as a sequence of bytes where every element is
     * {@code sizePerElement} bytes.
     * 
     * @param collection
     * @param sizePerElement
     * @return a byte array
     */
    public static ByteBuffer toByteBuffer(
            Collection<? extends Byteable> collection, int sizePerElement) {
        int size = (collection.size() * sizePerElement) + 8;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(collection.size());
        buffer.putInt(sizePerElement);
        for (Byteable object : collection) {
            Preconditions.checkArgument(object.size() == sizePerElement,
                    "'%s' must be '%s' bytes but it is "
                            + "actually '%s' bytes",
                    object, sizePerElement, object.size());
            object.copyTo(buffer);
        }
        buffer.rewind();
        return buffer;
    }

    /**
     * An {@link Iterator} that traverses a byte buffer and returns sub
     * sequences. The iterator assumes that each sequence is preceded by a 4
     * byte integer (a peek) that specifies how many bytes should be read and
     * returned with the next sequence. The iterator will fail to return a next
     * element when its peek is less than 1 or the byte buffer has no more
     * elements.
     * 
     * @author Jeff Nelson
     */
    private static class ByteableCollectionIterator implements
            Iterator<ByteBuffer> {

        protected final ByteBuffer bytes;
        protected ByteBuffer next;

        /**
         * Construct a new instance.
         * 
         * @param bytes
         */
        protected ByteableCollectionIterator(ByteBuffer bytes) {
            this.bytes = bytes;
            readNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public ByteBuffer next() {
            ByteBuffer next = this.next;
            readNext();
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(
                    "This method is not supported.");
        }

        /**
         * Read the next element from {@code bytes}.
         */
        protected void readNext() {
            next = null;
            if(bytes.remaining() >= 4) {
                int peek = bytes.getInt();
                if(peek > 0 && bytes.remaining() >= peek) {
                    next = ByteBuffers.slice(bytes, bytes.position(), peek);
                    bytes.position(bytes.position() + peek);
                }
                else {
                    bytes.position(bytes.position() - 4);
                }
            }
        }
    }

    /**
     * An {@link Iterator} that traverses a byte array and returns sequences.
     * The iterator assumes that the first 4 bytes of the sequence specifies the
     * number of sequences, the next four bytes specify the size of each
     * sequence and the remaining bytes are the sequences over which to iterate.
     * The iterator will fail to return a next element when its has read up to
     * the specified number of sequences or the capacity of its byte buffer is
     * less than the specified sequence size.
     * 
     * @author Jeff Nelson
     */
    private static class FixedSizeByteableCollectionIterator
            extends ByteableCollectionIterator {

        private int nextSequence = 0;
        private final int numSequences;
        private final int sequenceSize;

        /**
         * Construct a new instance.
         * 
         * @param bytes
         */
        protected FixedSizeByteableCollectionIterator(ByteBuffer bytes) {
            super(bytes);
            this.numSequences = this.bytes.getInt();
            this.sequenceSize = this.bytes.getInt();
            readFixedNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public ByteBuffer next() {
            ByteBuffer next = this.next;
            readFixedNext();
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(
                    "This method is not supported.");
        }

        @Override
        protected void readNext() {} // Do nothing. I am not overriding this
                                     // method because it is called by the
                                     // parent constructor and its execution
                                     // would cause unexpected behaviour

        private void readFixedNext() {
            next = null;
            if(nextSequence < numSequences
                    && bytes.remaining() >= sequenceSize) {
                next = ByteBuffers.slice(bytes, bytes.position(), sequenceSize);
            }
        }
    }

}
