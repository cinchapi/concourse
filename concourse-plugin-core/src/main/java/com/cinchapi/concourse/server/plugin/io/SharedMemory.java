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
package com.cinchapi.concourse.server.plugin.io;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.google.common.base.Throwables;

/**
 * {@link SharedMemory} is an alternative for local socket communication between
 * two separate processes.
 * <p>
 * Separate processes can communicate via {@link SharedMemory} by passing
 * messages. One process simply {@link #write(ByteBuffer) writes} a message and
 * another one {@link #read() reads} it.
 * </p>
 * <p>
 * {@link SharedMemory} is not limited to back-and-forth communication. Any
 * number of processes can communicate using the same {@link SharedMemory}
 * segment and any one processes can write any number of messages or read as
 * many messages as available.
 * </p>
 * <h1>Writing Messages</h1>
 * <p>
 * A writer process must first acquire a lock that blocks other writes from the
 * shared memory segment (e.g. writers and readers can use the segment
 * concurrently). The lock is released after the message is written. The size of
 * a {@link SharedMemory} segment is dynamic and written messages are appended
 * to the end.
 * </p>
 * <h1>Reading Messages</h1>
 * <p>
 * A reader process must first acquire a lock that blocks readers from the
 * shared memory segment (e.g. writers and readers can use the segment
 * concurrently). The lock is released after the message is written. A message
 * can only be read by one process. Once it is read, it is removed.
 * </p>
 * <h2>Compaction</h2>
 * <p>
 * Any process that is using a {@link SharedMemory} segment can remove old
 * messages by calling the {@link #compact()} method.
 * </p>
 * <h1>Latency</h1>
 * <p>
 * This class attempt to strike a balance between efficiently handling low
 * latency messages and minimizing CPU usage. To that end, the {@link #read()}
 * method employs an algorithm that prefers to spin/busy-wait for new messages
 * when messages are written in about 2 seconds or less. If the average latency
 * between messages grows larger than 2 seconds, the {@link #read()} method will
 * block while wait for notifications from the underlying file system, which may
 * exhibit additional delay depending on the OS.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class SharedMemory {

    /**
     * The total number of spin cycles to conduct before moving onto the next
     * round of spins.
     */
    private static final int MAX_SPIN_CYCLES_PER_ROUND = 20;

    /**
     * The total number of rounds to conduct spin cycles in the {@link #read()}
     * method before going into a wait/notify cycle.
     */
    private static final int MAX_SPIN_ROUNDS = 20;

    /**
     * The number of bytes used to store metadata at the beginning of the file
     */
    private static final int METADATA_SIZE_IN_BYTES = 10;

    /**
     * The position in the {@link #channel} where the {@link #readLock()} byte
     * his held.
     */
    private static final int READ_LOCK_POSITION = 8;

    /**
     * The max average millisecond latency that is allowable for the
     * {@link #read()} method to {@link #preferBusyWait()} as opposed to relying
     * on notifications from the underlying file system.
     */
    private static final long SPIN_AVG_LATENCY_TOLERANCE_IN_MILLIS = 2000;

    /**
     * The total number of seconds to backoff (e.g. sleep) after a round of
     * spins.This value is chosen so that the total amount of time spent
     * spinning is about 2 seconds.
     */
    private static final int SPIN_BACKOFF_IN_MILLIS = 100;

    /**
     * The position in the {@link #channel} where the {@link #writeLock()} byte
     * his held.
     */
    private static final int WRITE_LOCK_POSITION = 9;

    /**
     * The underlying {@link FileChannel} for the memory's backing store.
     */
    private final FileChannel channel;

    /**
     * The location of of the shared memory.
     */
    private final String location;

    /**
     * A {@link MappedByteBuffer} that tracks the content of the shared memory
     * segment.
     */
    private MappedByteBuffer memory;

    /**
     * The relative position in {@link #memory} where a reader should begin
     * consuming the next message.
     */
    private final StoredInteger nextRead;

    /**
     * The relative position in {@link #memory} where a writer should begin
     * storing the next message.
     */
    private final StoredInteger nextWrite;

    /**
     * The number of messages that have been read. This statistic is tracked for
     * potential optimizations.
     */
    private long readCount;

    /**
     * The total amount of time that this instance has ever waited after trying
     * to read a message.
     */
    private long totalLatency;

    /**
     * A local "lock" that indicates whether a local thread is writing. This is
     * to prevent multiple local threads from trying to grab the file lock.
     */
    private final AtomicBoolean writing = new AtomicBoolean(false);

    /**
     * Construct a new {@link SharedMemory} instance backed by a temporary
     * store.
     */
    public SharedMemory() {
        this(FileOps.tempFile("con", ".sm"), 1024);
    }

    /**
     * Construct a new instance.
     * 
     * @param path
     */
    public SharedMemory(String path) {
        this(path, 1024);
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
            this.location = path;
            this.channel = FileChannel.open(f.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.nextRead = new StoredInteger(channel, 0);
            this.nextWrite = new StoredInteger(channel, 4);
            this.memory = channel.map(MapMode.READ_WRITE,
                    METADATA_SIZE_IN_BYTES, capacity);
            if(nextWrite.get() == 0) {
                nextRead.setAndSync(-1);
            }
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
     * Run compact on the {@link SharedMemory} to occupy how much space is
     * utilized by removing garbage.
     */
    public void compact() {
        FileLock lock = lock();
        try {
            int position = nextRead.get();
            int garbage = position;
            memory.position(position);
            memory.compact();
            nextRead.addAndGet(-garbage);
            nextWrite.addAndGet(-garbage);
            nextWrite.sync();
            nextRead.sync();
            memory.force();

        }
        finally {
            FileLocks.release(lock);
        }
    }

    /**
     * Read the most recent message from the memory segment, blocking until a
     * message is available.
     * 
     * @return a {@link ByteBuffer} that contains the most recent message
     */
    public ByteBuffer read() {
        long start = System.currentTimeMillis();
        if(preferBusyWait()) {
            for (int i = 0; i < MAX_SPIN_ROUNDS; ++i) {
                int spins = 0;
                while (nextRead.get() < 0 && spins < MAX_SPIN_CYCLES_PER_ROUND) {
                    spins++;
                    continue;
                }
                if(spins < MAX_SPIN_CYCLES_PER_ROUND) {
                    break;
                }
                else {
                    try {
                        Thread.sleep(SPIN_BACKOFF_IN_MILLIS);
                    }
                    catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
        while (nextRead.get() < 0) {
            FileOps.awaitChange(location);
        }
        FileLock lock = null;
        try {
            lock = readLock();
            int position = nextRead.get();
            if(position >= 0) {
                long elapsed = System.currentTimeMillis() - start;
                totalLatency += elapsed;
                memory.position(position);
                int length = memory.getInt();
                ByteBuffer data = read(length);
                int mark = memory.position();
                int next = -1;
                try { // Peek at the next 4 bytes to see if it is > 0, which
                      // indicates that there is a next message to read.
                    int peek = memory.getInt();
                    if(peek > 0) {
                        next = mark;
                    }
                }
                catch (BufferUnderflowException e) {/* no-op */}
                memory.position(mark);
                nextRead.setAndSync(next);
                return data;
            }
            else { // race condition, someone else read the message before we
                   // did.
                return read();

            }
        }
        finally {
            FileLocks.release(lock);
        }
    }

    /**
     * Attempt to read and return a new message from the memory segment, if a
     * new one is available. If not, return {@code null}.
     * 
     * @return a {@link ByteBuffer} that contains the most recent message or
     *         {@code null} if no new message is available
     */
    @Nullable
    public ByteBuffer tryRead() {
        if(nextRead.get() >= 0) {
            FileLock lock = null;
            try {
                lock = tryReadLock();
                int length = 0;
                if(lock != null && nextRead.get() >= 0) {
                    return read(length);
                }
            }
            finally {
                FileLocks.release(lock);
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
        while (!writing.compareAndSet(false, true)) {
            continue;
        }
        while (data.capacity() + 4 > memory.remaining()) {
            grow();
        }
        FileLock lock = writeLock();
        try {
            int position = nextWrite.get();
            memory.position(position);
            int mark = memory.position();
            memory.putInt(data.capacity());
            memory.put(ByteBuffers.rewind(data));
            nextWrite.setAndSync(memory.position());
            // Check to see if the nextRead is < 0, in which case we must set it
            // equal to the position of the message that was just written
            if(nextRead.get() < 0) {
                nextRead.setAndSync(mark);// fsync is necessary in case reader
                                          // is waiting on filesystem
                                          // notification
            }
            return this;
        }
        finally {
            FileLocks.release(lock);
            writing.set(false);
        }
    }

    /**
     * Increase the capacity of the {@link #memory} segment.
     */
    private void grow() {
        FileLock lock = lock();
        try {
            growUnsafe();
        }
        finally {
            FileLocks.release(lock);
        }
    }

    /**
     * Increase the capacity of the {@link #memory} segment without grabbing the
     * lock.
     */
    private void growUnsafe() {
        try {
            int position = memory.position();
            memory = channel.map(MapMode.READ_WRITE, METADATA_SIZE_IN_BYTES,
                    memory.capacity() * 4);
            memory.position(position);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return an exclusive {@link FileLock} over the entire {@link #channel}.
     * 
     * <p>
     * Release the lock using {@link FileLocks#release(FileLock)}.
     * </p>
     * 
     * @return a {@link FileLock} over the entire channel
     */
    private FileLock lock() {
        try {
            int position = METADATA_SIZE_IN_BYTES - 2;
            return channel.lock(position, channel.size() - position, false);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return {@code true} if the {@link #read()} method should try busy waiting
     * before giving up its timeslice and relying on a notification from the
     * underlying filesystem.
     * <p>
     * This is done in an attempt to strike a balance between low latency reads
     * and high CPU usage.
     * </p>
     * 
     * @return {@code true} if busy waiting is preferable
     */
    private boolean preferBusyWait() {
        return readCount > 0 ? totalLatency / readCount <= SPIN_AVG_LATENCY_TOLERANCE_IN_MILLIS
                : true;
    }

    /**
     * Internal method to read {@code length} bytes from the {@link #memory}
     * segment, growing if necessary;
     */
    private ByteBuffer read(int length) {
        while (length > memory.remaining()) {
            growUnsafe();
        }
        ByteBuffer data = ByteBuffers.get(memory, length);
        return ByteBuffers.rewind(data);
    }

    /**
     * Return an exclusive {@link FileLock} that blocks other readers.
     * 
     * <p>
     * Release the lock using {@link FileLocks#release(FileLock)}.
     * </p>
     * 
     * @return a {@link FileLock} that blocks readers
     */
    private FileLock readLock() {
        try {
            return channel.lock(READ_LOCK_POSITION, 1, false);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Try to retrieve an excuslive {@link FileLock} that blocks other readers.
     * If the lock cannot be retrieved, return {@code null}.
     * 
     * <p>
     * Release the lock using {@link FileLocks#release(FileLock)}.
     * </p>
     * 
     * @return a {@like FileLock} that blocks readers or {@code null} if such a
     *         lock cannot be retrieved
     */
    private FileLock tryReadLock() {
        try {
            return channel.tryLock(READ_LOCK_POSITION, 1, false);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return an exclusive {@link FileLock} that blocks other writers.
     * 
     * <p>
     * Release the lock using {@link FileLocks#release(FileLock)}.
     * </p>
     * 
     * @return a {@link FileLock} that blocks writers
     */
    private FileLock writeLock() {
        try {
            return channel.lock(WRITE_LOCK_POSITION, 1, false);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
