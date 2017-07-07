/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Stopwatch;

/**
 * A {@link ReadWriteSharedLock} is a form of a reentrant lock that permits
 * either multiple concurrent readers OR multiple concurrent writers.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class ReadWriteSharedLock extends ReentrantReadWriteLock {

    /**
     * These are the internal locks that are used to actually control concurrent
     * access. We use a distinct traditional ReentrantReadWriteLock to represent
     * the read and write locks. In both cases, we only use the read view of the
     * locks so that multiple actors can concurrent access.
     */
    private final ReentrantReadWriteLock readers = new ReentrantReadWriteLock();

    /**
     * These are the internal locks that are used to actually control concurrent
     * access. We use a distinct traditional ReentrantReadWriteLock to represent
     * the read and write locks. In both cases, we only use the read view of the
     * locks so that multiple actors can concurrent access.
     */
    private final ReentrantReadWriteLock writers = new ReentrantReadWriteLock();

    /**
     * Return from the {@link #readLock()} method.
     */
    private final ReadLock readLock = new SharedReadLock();

    /**
     * Return from the {@link #writeLock()} method.
     */
    private final WriteLock writeLock = new SharedWriteLock();

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
        return writers.getReadHoldCount();
    }

    /**
     * Return the number write locks held for this lock. This method is designed
     * for use in monitoring systems, not for synchronization control.
     * 
     * @return the number of write locks held
     */
    public int getWriteLockCount() {
        return writers.getReadLockCount();
    }

    /**
     * Return {@code true} if the write lock is held by the current thread.
     * 
     * @return {@code true} if the write lock is held by the current thread
     */
    public boolean isWriteLockedByCurrentThread() {
        return writers.getReadHoldCount() > 0;
    }

    @Override
    public ReadLock readLock() {
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
    public WriteLock writeLock() {
        return writeLock;
    }

    /**
     * An {@link WriteLock} that can be grabbed by multiple writers at the same
     * time while blocking all readers.
     * 
     * @author Jeff Nelson
     */
    private class SharedWriteLock extends WriteLock {

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        protected SharedWriteLock() {
            super(ReadWriteSharedLock.this);
        }

        @Override
        public void lock() {
            boolean locked = false;
            while (!locked) {
                readers.writeLock().lock();
                locked = writers.readLock().tryLock();
                readers.writeLock().unlock();
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            boolean locked = false;
            while (!locked) {
                readers.writeLock().lockInterruptibly();
                locked = writers.readLock().tryLock();
                readers.writeLock().unlock();
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public boolean tryLock() {
            for (;;) {
                if(readers.getReadLockCount() == 0) {
                    if(readers.writeLock().tryLock()) {
                        try {
                            if(writers.readLock().tryLock()) {
                                return true;
                            }
                        }
                        finally {
                            readers.writeLock().unlock();
                        }
                        Thread.yield();
                    }
                }
                else {
                    return false;
                }
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            Stopwatch watch = Stopwatch.createStarted();
            for (;;) {
                if(readers.getReadLockCount() == 0) {
                    if(readers.writeLock().tryLock(time, unit)) {
                        watch.stop();
                        long elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        try {
                            if(writers.readLock().tryLock(time, unit)) {
                                return true;
                            }
                        }
                        finally {
                            readers.writeLock().unlock();
                        }
                        watch.stop();
                        elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        Thread.yield();
                    }
                }
                else {
                    return false;
                }
            }
        }

        @Override
        public void unlock() {
            writers.readLock().unlock();
        }

        @Override
        public Condition newCondition() {
            return writers.readLock().newCondition();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return writers.getReadHoldCount() > 0;
        }

        @Override
        public int getHoldCount() {
            return writers.getReadHoldCount();
        }
    }

    /**
     * An {@link ReadLock} that can be grabbed by multiple readers at the same
     * time while blocking all writers.
     * 
     * @author Jeff Nelson
     */
    private class SharedReadLock extends ReadLock {

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        protected SharedReadLock() {
            super(ReadWriteSharedLock.this);
        }

        @Override
        public void lock() {
            boolean locked = false;
            while (!locked) {
                if(writers.getReadHoldCount() > 0) {
                    // this thread already has the write lock, so it can
                    // reentrantly grab the read lock
                    locked = readers.readLock().tryLock();
                }
                else {
                    writers.writeLock().lock();
                    locked = readers.readLock().tryLock();
                    writers.writeLock().unlock();
                }
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            boolean locked = false;
            while (!locked) {
                if(writers.getReadHoldCount() > 0) {
                    // this thread already has the write lock, so it can
                    // reentrantly grab the read lock
                    locked = readers.readLock().tryLock();
                }
                writers.writeLock().lockInterruptibly();
                locked = readers.readLock().tryLock();
                writers.writeLock().unlock();
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public boolean tryLock() {
            for (;;) {
                if(writers.getReadLockCount() == 0) {
                    if(writers.getReadHoldCount() > 0) {
                        // this thread already has the write lock, so it can
                        // reentrantly grab the read lock
                        readers.readLock().lock();
                        return true;
                    }
                    else if(writers.writeLock().tryLock()) {
                        try {
                            if(readers.readLock().tryLock()) {
                                return true;
                            }
                        }
                        finally {
                            writers.writeLock().unlock();
                        }
                        Thread.yield();
                    }
                }
                else {
                    return false;
                }
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            Stopwatch watch = Stopwatch.createStarted();
            for (;;) {
                if(writers.getReadLockCount() == 0) {
                    if(writers.getReadHoldCount() > 0) {
                        // this thread already has the write lock, so it can
                        // reentrantly grab the read lock
                        readers.readLock().lock();
                        return true;
                    }
                    if(writers.writeLock().tryLock(time, unit)) {
                        watch.stop();
                        long elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        try {
                            if(readers.readLock().tryLock(time, unit)) {
                                return true;
                            }
                        }
                        finally {
                            writers.writeLock().unlock();
                        }
                        watch.stop();
                        elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        Thread.yield();
                    }
                }
                else {
                    return false;
                }
            }
        }

        @Override
        public void unlock() {
            readers.readLock().unlock();
        }

        @Override
        public Condition newCondition() {
            return readers.readLock().newCondition();
        }

    }

}
