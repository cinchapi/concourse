/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A {@link ReadWriteLock} that permits either multiple concurrent readers
 * OR multiple concurrent writers, at a time.
 *
 * @author Jeff Nelson
 */
class SharedReadWriteLock implements ReadWriteLock {

    /**
     * Synchronizer
     */
    private final Sync sync = new Sync();

    /**
     * A {@link Lock} that allows multiple concurrent readers and blocks
     * writers.
     */
    private final Lock readLock = new SharedLock(-1);

    /**
     * A {@link Lock} that allows multiple concurrent writers and block readers.
     */
    private final Lock writeLock = new SharedLock(1);

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    @Override
    public String toString() {
        int state = sync.getCount();
        int reads = 0;
        int writes = 0;
        if(state > 0) {
            writes += Math.abs(state);
        }
        else if(state < 0) {
            reads += Math.abs(state);
        }
        return super.toString() + "[Write locks = " + writes + ", Read locks = "
                + reads + "]";
    }

    /**
     * Uses the {@link #sync} to to perform locking, for a given mode (e.g.,
     * read or write).
     * 
     * @author Jeff Nelson
     */
    private class SharedLock implements Lock {

        /**
         * <ul>
         * <li>-1 means increment the number of read locks</li>
         * <li>1 means increment the number of write locks</li>
         * </ul>
         */
        private final int mode;

        /**
         * Construct a new instance.
         * 
         * @param mode
         */
        SharedLock(int mode) {
            this.mode = mode;
        }

        @Override
        public void lock() {
            sync.acquireShared(mode);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sync.acquireSharedInterruptibly(mode);
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            return sync.tryAcquireShared(mode) > 0;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            return sync.tryAcquireSharedNanos(mode,
                    TimeUnit.NANOSECONDS.convert(time, unit));
        }

        @Override
        public void unlock() {
            sync.releaseShared(mode);
        }

    }

    /**
     * Internal synchronizer that facilitates the semantics of "shared" locking.
     * <p>
     * The internal state is tracked as follows:
     * <ul>
     * <li>A value of <strong>0</strong> indicates that the no one holds the
     * lock and it may be acquired by a reader or a writer</li>
     * <li>A <strong>positive value</strong> indicates how many writers
     * currently hold the lock and that the lock may be acquired by another
     * writer, but not by a reader</li>
     * <li>A <strong>negative value</strong> indicates how many readers
     * currently hold the lock (e.g., the absolute value of the state) and that
     * the lock may be acquired by another reader, but not by a writer</li>
     * </ul>
     * </p>
     *
     * @author Jeff Nelson
     */
    private static final class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 1L;

        /**
         * Construct a new instance.
         */
        public Sync() {
            setState(0);
        }

        @Override
        protected int tryAcquireShared(int mode) {
            for (;;) {
                int state = getState();
                if((mode < 0 && state <= 0) || (mode > 0 && state >= 0)) {
                    if(compareAndSetState(state, state + mode)) {
                        return 1;
                    }
                }
                else {
                    return -1;
                }
            }
        }

        @Override
        protected boolean tryReleaseShared(int mode) {
            for (;;) {
                int state = getState();
                if(state == 0) {
                    throw new IllegalMonitorStateException();
                }
                if(compareAndSetState(state, state - mode)) {
                    return true;
                }
            }
        }

        /**
         * Return the number of holds.
         * 
         * @return the current number of holds
         */
        public int getCount() {
            return getState();
        }

    }

}