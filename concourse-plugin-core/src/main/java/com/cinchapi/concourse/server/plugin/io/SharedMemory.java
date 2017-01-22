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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.plugin.concurrent.FileLocks;
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
public final class SharedMemory implements InterProcessCommunication {

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
    private static final int METADATA_SIZE_IN_BYTES = 10;

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
     * The amount of time to sleep while executing a lazy check for race
     * conditions while waiting for file system notifications.
     */
    private static final int DORMANT_SPIN_SLEEP_TIME_IN_MILLIS = 1500;

    /**
     * The position in the {@link #channel} where the {@link #readLock()} byte
     * his held.
     */
    private static final int READ_LOCK_POSITION = 8;

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
     * The relative position in {@link #memory} where a reader should begin
     * consuming the next message.
     */
    private final MappedAtomicInteger nextRead;

    /**
     * The relative position in {@link #memory} where a writer should begin
     * storing the next message.
     */
    private final MappedAtomicInteger nextWrite;

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
            this.nextRead = new MappedAtomicInteger(channel, 0);
            this.nextWrite = new MappedAtomicInteger(channel, 4);
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
        this.lastCompaction = System.currentTimeMillis();
    }

    /**
     * Run compact on the {@link SharedMemory} to optimize how much space is
     * utilized by removing garbage.
     */
    @Override
    public void compact() {
        FileLock lock = lock();
        try {
            int start = nextRead.get();
            int end = start < 0 ? 0 : nextWrite.get(); // If start < 0, there
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
                memory.flip();
                memory.put(data);
                memory.flip();
                nextRead.set(0);
                nextWrite.set(end - start);
            }
            else {
                length = 0;
                memory.position(0);
                memory.limit(0);
                nextRead.set(-1);
                nextWrite.set(0);
            }
            channel.truncate(METADATA_SIZE_IN_BYTES + length);
            memory = channel.map(MapMode.READ_WRITE, METADATA_SIZE_IN_BYTES,
                    length);
            nextRead.sync();
            nextWrite.sync();
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
    @Override
    public ByteBuffer read() {
        long start = System.currentTimeMillis();
        if(preferBusyWait()) {
            for (int i = 0; i < MAX_SPIN_ROUNDS; ++i) {
                int spins = 0;
                while (nextRead.get() < 0
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
        while (nextRead.get() < 0) {
            Thread parentThread = Thread.currentThread();
            raceConditionDetector.execute(() -> {
                // NOTE: There is a subtle race condition that may occur if a
                // write comes in between the #nextRead check above and when
                // FileOps#awaitChange registers the #location with the watch
                // service. To get around that, we have a separate thread check
                // #nextRead and touch the #location if a write did come in when
                // the race condition happened.
                while (parentThread.getState() == State.RUNNABLE) {
                    // Wait until the parent thread is blocking before detecting
                    // a race condition...
                    Thread.yield();
                    continue;
                }
                while (nextRead.get() < 0) {
                    try {
                        Thread.sleep(DORMANT_SPIN_SLEEP_TIME_IN_MILLIS);
                    }
                    catch (InterruptedException e) {
                        throw CheckedExceptions.throwAsRuntimeException(e);
                    }
                    continue;
                }
                if(parentThread.getState() == State.RUNNABLE) {
                    parentThread.interrupt();
                }
            });
            try {
                FileOps.awaitChangeInterruptibly(location.toString());
            }
            catch (InterruptedException e) {
                continue;
            }
        }
        FileLock lock = readLock();
        try {
            int position = nextRead.get();
            if(position >= 0) {
                long elapsed = System.currentTimeMillis() - start;
                totalLatency += elapsed;
                memory.position(position);
                if(memory.remaining() < 4) {
                    growUnsafe();
                }
                int length = memory.getInt();
                while (length > memory.remaining()) {
                    growUnsafe();
                }
                ByteBuffer message = ByteBuffers.get(memory, length);
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
                nextRead.setAndSync(next);
                return message;
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
                nextRead.get(), nextWrite.get());
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
    @Override
    public SharedMemory write(ByteBuffer data) {
        FileLock lock = writeLock();
        try {
            // Must check to see if the underlying file has been truncated by
            // compaction from another process or else manipulation of the
            // current #memory segment won't actually be preserved. Not sure if
            // this is a Java bug or not...
            if(channel.size() < memory.capacity()) {
                memory = channel.map(MapMode.READ_WRITE, METADATA_SIZE_IN_BYTES,
                        channel.size() - METADATA_SIZE_IN_BYTES);
            }
            int position = nextWrite.get();
            while ((position > memory.limit()) || data.capacity() + 4 > memory
                    .position(position).remaining()) {
                growUnsafe();
            }
            int mark = memory.position();
            memory.putInt(data.capacity());
            memory.put(data);
            if(nextRead.get() < 0) {
                nextRead.setAndSync(mark);
            }
            nextWrite.setAndSync(memory.position());
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
     * Increase the capacity of the {@link #memory} segment without grabbing the
     * lock.
     */
    private void growUnsafe() {
        try {
            int position = memory.position();
            int capacity = Math.max(memory.capacity(), 1);
            memory = channel.map(MapMode.READ_WRITE, METADATA_SIZE_IN_BYTES,
                    capacity * 4);
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
        FileLock read = readLock();
        FileLock write = writeLock();
        return new FileLock(channel, READ_LOCK_POSITION, 2, false) {

            boolean valid = true;

            @Override
            public boolean isValid() {
                return valid;
            }

            @Override
            public void release() throws IOException {
                FileLocks.release(read);
                FileLocks.release(write);
                valid = false;
            }

        };

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
     * Return an exclusive {@link FileLock} that blocks other readers.
     * 
     * <p>
     * Release the lock using {@link FileLocks#release(FileLock)}.
     * </p>
     * 
     * @return a {@link FileLock} that blocks readers
     */
    private FileLock readLock() {
        return FileLocks.lock(channel, READ_LOCK_POSITION, 1, false);
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
        return FileLocks.lock(channel, WRITE_LOCK_POSITION, 1, false);
    }

}
