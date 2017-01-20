/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.io.IOException;
import java.lang.Thread.State;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
 * Old messages are removed from the segment during certain read and write
 * operations after the {@link #COMPACTION_FREQUENCY_IN_MILLIS} has passed since
 * the last compaction. While compaction will reduce the amount of disk space
 * used for the segment, each individual process will need to run a compaction
 * for the results to be visible in its memory space.
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
     * The amount of time before another compaction is done after a read or
     * write operation.
     */
    @VisibleForTesting
    protected static int COMPACTION_FREQUENCY_IN_MILLIS = 60000;

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
    private static final int METADATA_SIZE_IN_BYTES = 9;

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
     * The location of the locking byte in the underlying {@link #channel} and
     * {@link #memory}.
     */
    private static final int X_PROC_LOCK_POSITION = 0;

    /**
     * The location of the 32-bit sequence that contains the address where the
     * next write should begin.
     */
    private static final int NEXT_WRITE_ADDRESS_POSITION = 1;

    /**
     * The location of the 32-bit sequence that contains the address where the
     * next read should begin.
     */
    private static final int NEXT_READ_ADDRESS_POSITION = 5;

    /**
     * The underlying {@link FileChannel} for the memory's backing store.
     */
    private final FileChannel channel;

    /**
     * An executor service dedicated to running compaction in the background
     * after certain read or write operations.
     */
    private final ExecutorService compactor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("shared-memory-compactor")
                    .setDaemon(true).build());

    /**
     * The timestamp (in milliseconds) of the last compaction.
     */
    private long lastCompaction;

    /**
     * The location of of the shared memory.
     */
    private final Path location;

    /**
     * A {@link MappedByteBuffer} that tracks the content of the shared memory
     * segment.
     */
    private MappedByteBuffer memory;

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
     * An {@link Executor} dedicated to detecting and fixing race conditions.
     */
    private final ExecutorService raceConditionDetector = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("shared-memory-race-condition-detector")
                    .setDaemon(true).build());

    /**
     * A table intra-JVM lock that is used to ensure that threads have the same
     * protected access that multiple JVMs do as a result of the file locking
     * protocol.
     */
    private static final ConcurrentHashMap<Path, StampedLock> THREAD_LOCK_TABLE = new ConcurrentHashMap<>();

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
        capacity = Math.max(capacity, METADATA_SIZE_IN_BYTES + capacity);
        try {
            this.location = Paths.get(path).toAbsolutePath();
            this.channel = FileChannel.open(location, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.memory = channel.map(MapMode.READ_WRITE, 0, capacity);
            if(nextWriteAddr() == METADATA_SIZE_IN_BYTES) {
                nextReadAddr(-1);
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
        this.lastCompaction = System.currentTimeMillis();
        THREAD_LOCK_TABLE.putIfAbsent(location, new StampedLock());
    }

    /**
     * Run compact on the {@link SharedMemory} to optimize how much space is
     * utilized by removing garbage.
     */
    public void compact() {
        FileLock lock = null;
        try {
            lock = lock();
            int start = nextReadAddr();
            int end = start < 0 ? 0 : nextWriteAddr(); // If start < 0, there
                                                       // are no unread writes,
                                                       // so we can truncate the
                                                       // entire file
            int length;
            if(start >= 0) {
                length = end - start;
                memory.position(start);
                byte[] data = new byte[length];
                if(length > memory.remaining()) {
                    // There is more data in the underlying file than is
                    // represented in memory, so first grow to capture all of
                    // it.
                    growUnsafe();
                }
                memory.get(data);
                memory.position(METADATA_SIZE_IN_BYTES);
                memory.put(data);
                nextReadAddr(0);
                nextWriteAddr(memory.position());
            }
            else {
                length = 0;
                memory.position(0);
                memory.limit(METADATA_SIZE_IN_BYTES);
                nextReadAddr(-1);
                nextWriteAddr(0);
            }
            int size = METADATA_SIZE_IN_BYTES + length;
            channel.truncate(size);
            memory = channel.map(MapMode.READ_WRITE, 0, size);
            memory.force();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            lastCompaction = System.currentTimeMillis();
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
                while (nextReadAddr() < 0
                        && spins < MAX_SPIN_CYCLES_PER_ROUND) {
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
        while (nextReadAddr() < 0) {
            Thread parentThread = Thread.currentThread();
            raceConditionDetector.execute(() -> {
                // NOTE: There is a subtle race condition that may occur if a
                // write comes in between the #nextRead check above and when
                // FileOps#awaitChange registers the #location with the watch
                // service. To get around that, we have a separate thread check
                // #nextRead and touch the #location if a write did come in when
                // the race condition happened.
                while (parentThread.getState() == State.RUNNABLE) {
                    Thread.yield();
                    continue;
                }
                if(nextReadAddr() >= 0) {
                    FileOps.touch(location.toString());
                }
            });
            FileOps.awaitChange(location.toString());
        }
        FileLock lock = lock();
        try {
            int position = nextReadAddr();
            if(position >= 0) {
                long elapsed = System.currentTimeMillis() - start;
                totalLatency += elapsed;
                return readAt(position);
            }
            else { // race condition, someone else read the message before we
                   // did.
                return read();
            }
        }
        finally {
            FileLocks.release(lock);
            if(System.currentTimeMillis()
                    - lastCompaction > COMPACTION_FREQUENCY_IN_MILLIS) {
                compactor.execute(() -> {
                    compact();
                });
            }
            ++readCount;
        }
    }

    @Override
    public String toString() {
        return Strings.format(
                "SharedMemory[path={}, nextRead={}, nextWrite={}]", location,
                nextReadAddr(), nextWriteAddr());
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
        FileLock lock = lock();
        try {
            // Must check to see if the underlying file has been truncated by
            // compaction from another process or else manipulation of the
            // current #memory segment won't actually be preserved. Not sure if
            // this is a Java bug or not...
            if(channel.size() < memory.capacity()) {
                memory = channel.map(MapMode.READ_WRITE, 0, channel.size());
            }
            int address = nextWriteAddr();
            while ((address > memory.limit()) || data.capacity() + 4 > memory
                    .position(address).remaining()) {
                growUnsafe();
            }
            int mark = memory.position();
            memory.putInt(data.capacity());
            memory.put(ByteBuffers.rewind(data));
            nextWriteAddr(memory.position());
            // Check to see if the nextRead is < 0, in which case we must set it
            // equal to the position of the message that was just written
            if(nextReadAddr() < 0) {
                nextReadAddr(mark);
            }
            memory.force(); // fsync is necessary in case reader is waiting on
                            // filesystem notification

            return this;

        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
        finally {
            FileLocks.release(lock);
            if(System.currentTimeMillis()
                    - lastCompaction > COMPACTION_FREQUENCY_IN_MILLIS) {
                compactor.execute(() -> {
                    compact();
                });
            }
        }
    }

    /**
     * Internal method to read {@code length} bytes from the {@link #memory}
     * segment, growing if necessary;
     */
    private ByteBuffer doReadFromCurrentPosition(int length) {
        while (length > memory.remaining()) {
            growUnsafe();
        }
        ByteBuffer data = ByteBuffers.get(memory, length);
        return ByteBuffers.rewind(data);
    }

    /**
     * Increase the capacity of the {@link #memory} segment without grabbing the
     * lock.
     */
    private void growUnsafe() {
        try {
            int position = memory.position();
            int capacity = Math.max(memory.capacity(), 1);
            memory = channel.map(MapMode.READ_WRITE, 0, capacity * 4);
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
        StampedLock threadLock = THREAD_LOCK_TABLE.get(location);
        long stamp = threadLock.writeLock();
        try {
            FileLock internal = channel.lock(X_PROC_LOCK_POSITION, 1, false);
            return new FileLock(channel, X_PROC_LOCK_POSITION, 1, false) {

                @Override
                public boolean isValid() {
                    return internal.isValid();
                }

                @Override
                public void release() throws IOException {
                    FileLocks.release(internal);
                    threadLock.unlockWrite(stamp);
                }

            };
        }
        catch (OverlappingFileLockException | IOException e) {
            // NOTE: The #threadLock should prevent a
            // OverlappingFileLockException from ever being thrown.
            throw CheckedExceptions.throwAsRuntimeException(e);
        }

    }

    /**
     * Internal method to get the address where the next read should begin.
     * 
     * @return the address of the next read
     */
    private int nextReadAddr() {
        return readAddr(NEXT_READ_ADDRESS_POSITION);
    }

    /**
     * Internal method to set the address where the next read should begin.
     * 
     * @param address
     */
    private void nextReadAddr(int address) {
        setAddr(NEXT_READ_ADDRESS_POSITION, address);
    }

    /**
     * Internal method to get the address where the next write should begin.
     * 
     * @return the address for the next write
     */
    private int nextWriteAddr() {
        return readAddr(NEXT_WRITE_ADDRESS_POSITION);
    }

    /**
     * Internal method to set the address where the next write should begin.
     * 
     * @param address
     */
    private void nextWriteAddr(int address) {
        setAddr(NEXT_WRITE_ADDRESS_POSITION, address);
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
        return readCount > 0
                ? totalLatency
                        / readCount <= SPIN_AVG_LATENCY_TOLERANCE_IN_MILLIS
                : true;
    }

    /**
     * Internal method to read an address index.
     * 
     * @param position
     * @return
     */
    private int readAddr(int position) {
        int current = memory.position();
        memory.position(position);
        int address = memory.getInt();
        memory.position(current);
        if(address == 0) {
            address += METADATA_SIZE_IN_BYTES;
        }
        return address;
    }

    /**
     * Perform a read at {@code position} in the {@link #memory} segment.
     * 
     * @param position the position at which the read starts
     * @return the data at the position
     */
    private ByteBuffer readAt(int position) {
        memory.position(position);
        if(memory.remaining() < 4) {
            growUnsafe();
        }
        int length = memory.getInt();
        ByteBuffer data = doReadFromCurrentPosition(length);
        int mark = memory.position();
        int next = -1;
        boolean retry = true;
        while (retry) {
            retry = false;
            try { // Peek at the next 4 bytes to see if it is > 0, which
                  // indicates that there is a next message to read.
                int peek = memory.getInt();
                if(peek > 0) {
                    next = mark;
                }
            }
            catch (BufferUnderflowException e) {
                growUnsafe();
                retry = true;
            }
        }

        memory.position(mark);
        nextReadAddr(next);
        memory.force();
        return data;
    }

    /**
     * Internal method to set an address index.
     * <p>
     * CAUTION: This method should only be called after grabbing the
     * {@link #lock()}.
     * </p>
     * 
     * @param position
     * @param address
     */
    private void setAddr(int position, int address) {
        int current = memory.position();
        memory.position(position);
        memory.putInt(address);
        memory.position(current);
    }

}
