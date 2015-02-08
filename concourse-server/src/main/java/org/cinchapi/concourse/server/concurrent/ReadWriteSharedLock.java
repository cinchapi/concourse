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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Stopwatch;

/**
 * A {@link ReadWriteSharedLock} is a form of a reentrant lock that permits
 * either multiple concurrent readers OR multiple concurrent writers.
 * 
 * @author jnelson
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
     * @author jnelson
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
                        if(writers.readLock().tryLock()) {
                            return true;
                        }
                        readers.writeLock().unlock();
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
                        if(writers.readLock().tryLock(time, unit)) {
                            return true;
                        }
                        readers.writeLock().unlock();
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
     * @author jnelson
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
                        if(readers.readLock().tryLock()) {
                            return true;
                        }
                        writers.writeLock().unlock();
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
                        if(readers.readLock().tryLock(time, unit)) {
                            return true;
                        }
                        writers.writeLock().unlock();
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
