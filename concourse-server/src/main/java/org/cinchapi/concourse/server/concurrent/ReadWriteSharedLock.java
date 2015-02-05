/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Stopwatch;

/**
 * A {@link ReadWriteSharedLock} is a form of a reentrant lock that permits
 * either multiple concurrent readers OR multiple concurrent writers.
 * 
 * @author jnelson
 */
public class ReadWriteSharedLock implements ReadWriteLock {

    /**
     * These are the internal locks that are used to actually control concurrent
     * access. We use a distinct traditional ReentrantReadWriteLock to represent
     * the read and write locks. In both cases, we only use the read view of the
     * locks so that multiple actors can concurrent access.
     */
    private final ReentrantReadWriteLock readers, writers;

    /**
     * These are the actual read and write locks that are returned from the
     * methods of this lock.
     */
    private final Lock readLock, writeLock;

    /**
     * The are the syncs that are used internally to signal when a certain kind
     * of actor is no longer blocked.
     */
    private final Object readSync, writeSync;

    /**
     * The flag that indicates whether this lock allows many concurrent writers
     * (traditional = false) or not (traditional = true). This variable is
     * called traditional because, in the case that it is {@code true}, this
     * lock behaves just like a normal {@link ReentrantReadWriteLock}.
     */
    private final boolean traditional;

    /**
     * Construct a new default instance which allows multiple concurrent
     * writers.
     */
    public ReadWriteSharedLock() {
        this(true);
    }

    /**
     * Construct a new instance that optionally allows concurrent writers or not
     * (and therefore behaves like a traditional {@link ReentrantReadWriteLock}.
     * 
     * @param allowConcurrentWriters
     */
    public ReadWriteSharedLock(boolean allowConcurrentWriters) {
        this.traditional = !allowConcurrentWriters;
        if(traditional) {
            this.readers = new ReentrantReadWriteLock();
            this.writers = readers;
            this.readLock = readers.readLock();
            this.writeLock = readers.writeLock();
            this.readSync = null;
            this.writeSync = null;
        }
        else {
            this.readers = new ReentrantReadWriteLock();
            this.writers = new ReentrantReadWriteLock();
            this.readSync = new Object();
            this.writeSync = new Object();
            this.readLock = new SharedLock(readers, writers, readSync,
                    writeSync);
            this.writeLock = new SharedLock(writers, readers, writeSync,
                    readSync);
        }

    }

    /**
     * Return the number of reentrant read holds on this lock by the current
     * thread. A reader thread has a hold on a lock for each lock action that is
     * not matched by an unlock action.
     * 
     * @return the number of read holds on the read lock by the current thread,
     *         or zero if the read lock is not held by the current thread
     */
    public int getReadHoldCount() {
        return readers.getReadHoldCount();
    }

    /**
     * Return the number read locks held for this lock. This method is designed
     * for use in monitoring systems, not for synchronization control.
     * 
     * @return the number of read locks held
     */
    public int getReadLockCount() {
        return readers.getReadLockCount();
    }

    /**
     * Return the number of reentrant write holds on this lock by the current
     * thread. A writer thread has a hold on a lock for each lock action that is
     * not matched by an unlock action.
     * 
     * @return the number of write holds on the read lock by the current thread,
     *         or zero if the write lock is not held by the current thread
     */
    public int getWriteHoldCount() {
        return traditional ? writers.getWriteHoldCount() : writers
                .getReadHoldCount();
    }

    /**
     * Return the number write locks held for this lock. This method is designed
     * for use in monitoring systems, not for synchronization control.
     * 
     * @return the number of write locks held
     */
    public int getWriteLockCount() {
        return traditional ? (writers.isWriteLocked() ? 1 : 0) : writers
                .getReadLockCount();
    }

    /**
     * Return {@code true} if the write lock is held by the current thread.
     * 
     * @return {@code true} if the write lock is held by the current thread
     */
    public boolean isWriteLockedByCurrentThread() {
        return traditional ? writers.isWriteLockedByCurrentThread() : writers
                .getReadHoldCount() > 0;
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass());
        sb.append("$");
        sb.append(System.identityHashCode(this));
        sb.append("[");
        sb.append("readers=");
        sb.append(readers.getReadLockCount());
        sb.append("(");
        sb.append(readers.isWriteLocked());
        sb.append(")");
        sb.append(", ");
        sb.append("writers=");
        sb.append(writers.getReadLockCount());
        sb.append("(");
        sb.append(writers.isWriteLocked());
        sb.append(")");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    /**
     * An view that represents a lock that can be grabbed by multiple actors of
     * the same kind (i.e. many readers can concurrently lock OR many writers
     * can concurrently lock).
     * 
     * @author jnelson
     */
    static class SharedLock implements Lock {

        /**
         * References to the internal locks that are coordinated by this shared
         * instance. {@link #offense} represents the lock that this instance is
         * trying to grab while {@link #defense} represents the other lock that
         * is trying to block it.
         */
        private final ReentrantReadWriteLock offense, defense;

        /**
         * References to the syncs that are used to notify the other internal
         * shared instance that they are no longer blocked.
         */
        private final Object receivers, corners;

        /**
         * This constructor is meant for anonymous subclasses to create a new
         * instance in-line.
         * 
         * @param parent
         * @param read
         */
        protected SharedLock(ReadWriteSharedLock parent, boolean read) {
            this(read ? parent.readers : parent.writers, read ? parent.writers
                    : parent.readers,
                    read ? parent.readSync : parent.writeSync,
                    read ? parent.writeSync : parent.readSync);
        }

        /**
         * Construct a new instance.
         * 
         * @param offense
         * @param defense
         * @param receivers
         * @param corners
         */
        private SharedLock(ReentrantReadWriteLock offense,
                ReentrantReadWriteLock defense, Object receivers, Object corners) {
            this.offense = offense;
            this.defense = defense;
            this.receivers = receivers;
            this.corners = corners;
        }

        @Override
        public void lock() {
            if(defense.getReadLockCount() > 0) {
                synchronized (receivers) {
                    try {
                        receivers.wait();
                        lock();
                    }
                    catch (InterruptedException e) {}
                }
            }
            else if(defense.writeLock().tryLock()) {
                try {
                    offense.readLock().lock();
                }
                finally {
                    defense.writeLock().unlock();
                }
            }
            else {
                lock();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            if(defense.getReadLockCount() > 0) {
                synchronized (receivers) {
                    receivers.wait();
                    lock();
                }
            }
            else if(defense.writeLock().tryLock()) {
                try {
                    offense.readLock().lock();
                }
                finally {
                    defense.writeLock().unlock();
                }
            }
            else {
                lock();
            }

        }

        @Override
        public Condition newCondition() {
            return offense.readLock().newCondition();
        }

        @Override
        public boolean tryLock() {
            if(defense.getReadLockCount() == 0) {
                if(defense.writeLock().tryLock()) {
                    try {
                        return offense.readLock().tryLock();
                    }
                    finally {
                        defense.writeLock().unlock();
                    }
                }
                else {
                    return tryLock();
                }
            }
            else {
                return false;
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            if(defense.getReadLockCount() != 0) {
                Stopwatch watch = Stopwatch.createStarted();
                synchronized (receivers) {
                    receivers.wait(TimeUnit.MILLISECONDS.convert(time, unit));
                }
                watch.stop();
                long elapsed = watch.elapsed(unit);
                long left = time - elapsed;
                if(left > 0) {
                    return tryLock(left, unit);
                }
                else {
                    return false;
                }

            }
            else {
                return offense.readLock().tryLock(time, unit);
            }
        }

        @Override
        public void unlock() {
            offense.readLock().unlock();
            if(offense.getReadLockCount() == 0) {
                synchronized (corners) {
                    corners.notifyAll();
                }
            }
        }

    }

}
