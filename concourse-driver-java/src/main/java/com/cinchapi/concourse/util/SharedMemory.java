/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;

/**
 * {@link SharedMemory} is an alternative for local socket communication to
 * facilitate message passing between two separate JVM processes.
 * <p>
 * A shared memory segment is only designed for communicate between exactly two
 * processes. Using it with more than two will yield unexpected results. If a
 * process needs to communicate with multiple other processes, it should
 * establish distinct shared memory segments with each of those processes.
 * </p>
 * <p>
 * Shared memory works via message passing. Processes synchronously pass each
 * other messages via the shared memory. As such, after a processes
 * {@link #write(ByteBuffer) writes} a message, it should wait for a response by
 * {@link #read() reading}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public final class SharedMemory {

    /**
     * A {@link MappedByteBuffer} that tracks the content of the shared memory
     * segment.
     */
    public MappedByteBuffer memory;

    /**
     * The underlying {@link FileChannel} for the memory's backing store.
     */
    private final FileChannel channel;

    /**
     * A buffer mapped to the first four bytes of the underlying
     * {@link #channel} to indicate the number of bytes in the next message to
     * be read.
     */
    private final MappedByteBuffer readLength;

    /**
     * Construct a new {@link SharedMemory} instance backed by a temporary
     * store.
     */
    public SharedMemory() {
        this(FileOps.tempFile("con", ".sm"), 1024);
    }

    /**
     * Construct a new {@link SharedMemory} instance backed by the file at
     * {@code path} with the specified initial {@code capacity}.
     * 
     * @param path the path of the backing file for the shared memory
     * @param capacity the initial capacity of the shared memory segment
     */
    public SharedMemory(String path, int capacity) {
        final File f = new File(path);
        try {
            this.channel = FileChannel.open(f.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.readLength = channel.map(MapMode.READ_WRITE, 0, 4);
            this.memory = channel.map(MapMode.READ_WRITE, 5, capacity);
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    try {
                        channel.close();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }

            });
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Read the most recent message from the memory segment, block until a
     * message is available.
     * 
     * @return a {@link ByteBuffer} that contains the most recent message
     */
    public ByteBuffer read() {
        while (ByteBuffers.rewind(readLength).getInt() <= 0) {
            continue; // busy wait until a message arrives
        }
        FileLock flock = null;
        try {
            flock = channel.lock();
            int length = 0;
            if((length = ByteBuffers.rewind(readLength).getInt()) > 0) {
                return read(length);
            }
            else { // race condition, someone else read the message before we
                   // did.
                return read();

            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if(flock != null) {
                try {
                    flock.release();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * Attempt to read and return a new message from the memory segment, if a
     * new
     * one is available. If not, return {@code null}.
     * 
     * @return a {@link ByteBuffer} that contains the most recent message or
     *         {@code null} if no new message is available
     */
    @Nullable
    public ByteBuffer tryRead() {
        if(ByteBuffers.rewind(readLength).getInt() > 0) {
            FileLock flock = null;
            try {
                flock = channel.tryLock();
                int length = 0;
                if(flock != null
                        && (length = ByteBuffers.rewind(readLength).getInt()) > 0) {
                    return read(length);
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                if(flock != null) {
                    try {
                        flock.release();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }

        }
        return null;
    }

    /**
     * Write {@code data} to the shared memory segment.
     * <p>
     * This method grabs an exclusive lock on the shared memory so that no other
     * readers or writers may access while the message is being written. As
     * such, this method also <strong>blocks</strong> while waiting for the
     * memory segment to become available for writing.
     * </p>
     * <p>
     * <strong>CAUTION:</strong> This method does not check to make sure that
     * the most recent message was read before writing.
     * </p>
     * 
     * @param data the message to write to the memory segment
     * @return {@link SharedMemory this}
     */
    public SharedMemory write(ByteBuffer data) {
        if(data.capacity() > memory.capacity()) {
            grow();
        }
        FileLock flock = null;
        try {
            flock = channel.lock();
            memory.position(0);
            memory.put(ByteBuffers.rewind(data));
            ByteBuffers.rewind(readLength).putInt(data.capacity());
            return this;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if(flock != null) {
                try {
                    flock.release();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * Increase the capacity of the {@link #memory} segment.
     */
    private void grow() {
        try {
            memory = channel.map(MapMode.READ_WRITE, memory.position(),
                    memory.capacity() * 4);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Internal method to read {@code length} bytes from the {@link #memory}
     * segment and reset the {@link #readLength} so other readers are forced to
     * wait for a new message.
     */
    private ByteBuffer read(int length) {
        if(length > memory.capacity()) {
            grow();
        }
        ByteBuffer data = ByteBuffers.get(ByteBuffers.rewind(memory), length);
        ByteBuffers.rewind(readLength).putInt(0);
        return ByteBuffers.rewind(data);
    }
}
