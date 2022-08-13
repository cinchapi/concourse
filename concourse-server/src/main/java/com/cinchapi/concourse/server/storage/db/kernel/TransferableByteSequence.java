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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.FileSystem;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link TransferableByteSequence} contains data that can be efficiently
 * {@link #transfer(Path, long) transferred} from memory to a file.
 * <p>
 * Even if the {@link #length() length} of the sequence (based on the
 * accumulation of its member components) is greater than
 * {@link Integer#MAX_VALUE}, a {@link TransferableByteSequence} can stream the
 * bytes from its data in batches without buffering the entire sequence in
 * memory.
 * </p>
 * <p>
 * A {@link TransferableByteSequence} is initially mutable and maintained in
 * memory. Once it is {@link #transfer(Path, long) transferred}, it becomes
 * immutable and it is expected that the memory for it's data immediately
 * becomes eligible for garbage collection. As a result, subsequent reads of
 * it's data are incrementally streamed from the file to which the sequence was
 * {@link #transfer(Path, long) transferred} on-demand.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
abstract class TransferableByteSequence implements Closeable {

    /**
     * The upper limit when there is performance stagnation or degradation when
     * using a {@link MappedByteBuffer} to {@link #transfer(Path, long)
     * transfer} the sequence and a {@link FileChannel} should be used instead.
     */
    private static int MMAP_TRANSFER_UPPER_LIMIT = 419430400;

    /**
     * The master lock for {@link #write} and {@link #read}. DO NOT use this
     * lock directly.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * A shared lock that permits many readers and no writer. Use this lock to
     * ensure that no data modification occurs when the sequence is
     * {@link #isMutable()} and data is being read.
     */
    protected final ReadLock read = master.readLock();

    /**
     * An exclusive lock that permits only one writer and no readers. Use this
     * lock to ensure that no read occurs while data is being modified while
     * this sequence is {@link #isMutable() mutable}.
     */
    protected final WriteLock write = master.writeLock();

    /**
     * A {@link FileChannel} for {@link #file}.
     */
    @Nullable
    private FileChannel channel;

    /**
     * The file where the sequence was {@link #transfer(Path, long)
     * transferred}. If the sequence is {@link #mutable}, this value is
     * {@code null}
     */
    @Nullable
    private Path file;

    /**
     * A flag that tracks whether the sequence is mutable (and therefore has not
     * been {@link #transfer(Path, long) transferred}) or not.
     */
    private boolean mutable;

    /**
     * The byte where this sequence begins in the underlying {@link #file}.
     * <p>
     * This value is unset until the sequence is {@link #transfer(Path, long)
     * transferred}.
     * </p>
     */
    private long position;

    /**
     * Construct a new instance.
     */
    protected TransferableByteSequence() {
        this.file = null;
        this.channel = null;
        this.mutable = true;
        this.position = -1;
    }

    /**
     * Load an existing sequence from {@code file}.
     * 
     * @param file
     */
    protected TransferableByteSequence(Path file) {
        this(file, 0, FileSystem.getFileSize(file.toString()));
    }

    /**
     * Load an existing sequence from {@code file}, starting at {@code position}
     * for {@code length} bytes.
     * 
     * @param file
     * @param position
     * @param length
     */
    protected TransferableByteSequence(Path file, long position, long length) {
        this(file, null, position, length);
    }

    /**
     * Load an existing sequence from {@code file}, starting at {@code position}
     * for {@code length} bytes.
     * 
     * @param file
     * @param position
     * @param length
     */
    protected TransferableByteSequence(Path file, FileChannel channel,
            long position, long length) {
        this.file = file;
        this.channel = channel;
        this.mutable = false;
        this.position = position;
    }

    @Override
    public void close() throws IOException {
        if(channel != null) {
            channel.close();
        }
    }

    /**
     * Return {@code true} if this sequence has not been
     * {@link #transfer(Path, long) transferred} and is therefore mutable.
     * Otherwise, return {@code false}
     * 
     * @return {@code true} if this sequence is mutable
     */
    public final boolean isMutable() {
        return mutable;
    }

    /**
     * Returns the total number of bytes in the sequence.
     * <p>
     * Since the sequence length can exceed {@link Integer#MAX_VALUE}
     * bytes, it is recommended that the value returned from this method is
     * tracked while the sequence is mutable instead of being dynamically
     * computed in a manner that requires the generation and of the full
     * sequence in memory.
     * <p>
     * 
     * @return the number of bytes.
     */
    public abstract long length();

    /**
     * Transfer the sequence to {@code file} and make it immutable.
     * <p>
     * When this method returns, it is guaranteed that all data represented by
     * this sequence have been written to disk.
     * </p>
     * 
     * @param file
     */
    public final void transfer(Path file) {
        transfer(file, 0);
    }

    /**
     * Transfer the sequence to {@code file}, starting at {@code position} and
     * make it immutable.
     * <p>
     * When this method returns, it is guaranteed that all data represented by
     * this sequence have been written to disk.
     * </p>
     * 
     * @param file
     * @param position
     */
    public final void transfer(Path file, long position) {
        Preconditions.checkState(mutable,
                "%s has already been transferred to %s", this, file);
        write.lock();
        try {
            long length = length();
            Preconditions.checkState(length >= 0,
                    "%s has a negative length: %s", file, length);
            CloseableByteSink sink;
            if(length <= MMAP_TRANSFER_UPPER_LIMIT) {
                MappedByteBuffer buffer = FileSystem.map(file,
                        MapMode.READ_WRITE, position, length);
                sink = new CloseableByteSink(file, buffer);
            }
            else {
                FileChannel channel = FileSystem.getFileChannel(file);
                channel.position(position);
                // NOTE: The below FileLock is automatically released when
                // calling #sink.close()
                channel.lock(position, length, false);
                sink = new CloseableByteSink(file, channel);
            }
            try {
                transfer(sink);
                sink.force();
                sink.free();
            }
            finally {
                sink.close();
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            write.unlock();
        }
    }

    /**
     * If a backing {@link #file} is available, return a {@link FileChannel} to
     * it that is {@link StandardOpenOption#READ read only}.
     * 
     * @return the backing {@link FileChannel}, if available
     */
    @Nullable
    protected final FileChannel channel() {
        if(file != null && channel == null) {
            try {
                channel = FileChannel.open(file, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        return channel;
    }

    /**
     * Return the file where the sequence was {@link #transfer(Path, long)
     * transferred}. If the sequence is {@link #mutable}, this value is
     * {@code null}.
     * 
     * @return the backing file, if available
     */
    @Nullable
    protected final Path file() {
        return file;
    }

    /**
     * Write the bytes in this sequence to {@code sink}.
     * 
     * <p>
     * If a member component is a {@link TransferableByteSink}, call
     * {@link #transfer(ByteSink)} to copy the sequence to the <strong> same
     * {@code sink} </strong> instead of calling this method, which does not
     * register the destination file or position or calling
     * {@link #transfer(Path, long)} which creates a new {@link ByteSink}.
     * </p>
     * 
     * @param sink
     */
    protected abstract void flush(ByteSink sink);

    /**
     * Free memory that was being used to hold this sequence's data while it was
     * {@link #isMutable() mutable}.
     */
    protected abstract void free();

    /**
     * Return the position where this sequence begins in {@link #file} if it has
     * been {@link #transfer(Path, long) transferred}.
     * 
     * @return the position
     * @throws IllegalStateException if the position has not been set
     */
    protected final long position() throws IllegalStateException {
        if(position >= 0) {
            return position;
        }
        else {
            throw new IllegalStateException("The position has not been set");
        }
    }

    /**
     * Transfer the sequence to the {@code sink} and make it immutable.
     * 
     * <p>
     * This method is designed to be called from {@link #flush(ByteSink)} when a
     * member component is also a {@link TransferableByteSequence} and the
     * destination {@link ByteSink} should be shared. As such, this method does
     * not force the sequence onto disk.
     * </p>
     * 
     * @param sink
     */
    protected final void transfer(ByteSink sink) {
        Preconditions.checkState(mutable,
                "%s has already been transferred to %s", this, file);
        write.lock();
        try {
            CloseableByteSink cbs = (CloseableByteSink) sink;
            cbs.register(this);
            this.file = cbs.file();
            this.position = sink.position();
            flush(sink);
            this.mutable = false;
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Internal wrapper for a {@link ByteSink} that encapsulates the association
     * with a {@link Path file} and the ability to force and close the
     * destination resource.
     *
     * @author Jeff Nelson
     */
    private final static class CloseableByteSink implements ByteSink {

        /**
         * The associated file.
         */
        private final Path file;

        /**
         * The underlying resource.
         */
        private final Object resource;

        /**
         * The {@link TransferableByteSequence TransferableByteSequences} that
         * have been {@link #transfer(ByteSink) transferred} to this
         * {@link ByteSink} and will need to be {@link #free() freed}.
         */
        private final List<TransferableByteSequence> sequences = Lists
                .newArrayListWithCapacity(7);

        /**
         * The {@link ByteSink} that is wrapped.
         */
        private final ByteSink sink;

        /**
         * Construct a new instance.
         * 
         * @param file
         * @param channel
         */
        private CloseableByteSink(Path file, FileChannel channel) {
            this.sink = ByteSink.to(channel);
            this.file = file;
            this.resource = channel;
        }

        /**
         * Construct a new instance.
         * 
         * @param file
         * @param buffer
         */
        private CloseableByteSink(Path file, MappedByteBuffer buffer) {
            this.sink = ByteSink.to(buffer);
            this.file = file;
            this.resource = buffer;
        }

        @Override
        public long position() {
            return sink.position();
        }

        @Override
        public ByteSink put(byte value) {
            sink.put(value);
            return this;
        }

        @Override
        public ByteSink put(byte[] src) {
            sink.put(src);
            return this;
        }

        @Override
        public ByteSink put(ByteBuffer src) {
            sink.put(src);
            return this;
        }

        @Override
        public ByteSink putChar(char value) {
            sink.putChar(value);
            return this;
        }

        @Override
        public ByteSink putDouble(double value) {
            sink.putDouble(value);
            return this;
        }

        @Override
        public ByteSink putFloat(float value) {
            sink.putFloat(value);
            return this;
        }

        @Override
        public ByteSink putInt(int value) {
            sink.putInt(value);
            return this;
        }

        @Override
        public ByteSink putLong(long value) {
            sink.putLong(value);
            return this;
        }

        @Override
        public ByteSink putShort(short value) {
            sink.putShort(value);
            return this;
        }

        /**
         * Close the underlying resource.
         */
        protected void close() {
            if(resource instanceof FileChannel) {
                FileSystem.closeFileChannel((FileChannel) resource);
            }
            else if(resource instanceof MappedByteBuffer) {
                // Assume that the mapping should stay in memory for subsequent
                // reads. Also, It is unsafe to force unmap the buffer in Java.
                // But hold space for future possibility?
            }
            else {
                throw new IllegalStateException();
            }
        }

        /**
         * Return the {@link #file} associated with this sink.
         * 
         * @return the file
         */
        protected Path file() {
            return file;
        }

        /**
         * Force the {@link #resource} to flush the content to disk in
         * {@link #file}.
         */
        protected void force() {
            sink.flush();
            if(resource instanceof FileChannel) {
                try {
                    ((FileChannel) resource).force(true);
                }
                catch (IOException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }
            else if(resource instanceof MappedByteBuffer) {
                ((MappedByteBuffer) resource).force();
            }
            else {
                throw new IllegalStateException();
            }
        }

        /**
         * Free any {@link TransferableByteSequence TransferableByteSequences}
         * that have been transferred to this {@link ByteSink}.
         */
        protected void free() {
            for (TransferableByteSequence sequence : sequences) {
                sequence.free();
            }
        }

        /**
         * Register the {@code sequence} to later be {@link #free() freed}.
         * 
         * @param sequence
         */
        protected void register(TransferableByteSequence sequence) {
            this.sequences.add(sequence);
        }

    }

}
