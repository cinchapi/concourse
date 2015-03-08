/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import org.cinchapi.concourse.util.ByteBuffers;

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
                            + "actually '%s' bytes", object, sizePerElement,
                    object.size());
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
    private static class FixedSizeByteableCollectionIterator extends
            ByteableCollectionIterator {

        private final int numSequences;
        private final int sequenceSize;
        private int nextSequence = 0;

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
            if(nextSequence < numSequences && bytes.remaining() >= sequenceSize) {
                next = ByteBuffers.slice(bytes, bytes.position(), sequenceSize);
            }
        }
    }

}
