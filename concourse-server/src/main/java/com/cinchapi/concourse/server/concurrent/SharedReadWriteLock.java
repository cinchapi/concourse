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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import com.google.common.base.Stopwatch;

/**
 * A {@link ReadWriteLock} that permits either multiple concurrent readers
 * OR multiple concurrent writers, at a time.
 *
 * @author Jeff Nelson
 */
class SharedReadWriteLock implements ReadWriteLock {

    /**
     * An internal lock that controls concurrent access; allowing multiple
     * readers and blocking writers.
     */
    private final ReadWriteLock readers;

    /**
     * An internal lock that controls concurrent access; allowing multiple
     * writers and blocking readers.
     */
    private final ReadWriteLock writers;

    /**
     * Construct a new instance.
     */
    SharedReadWriteLock() {
        this.readers = new StampedLock().asReadWriteLock();
        this.writers = new StampedLock().asReadWriteLock();
    }

    @Override
    public Lock readLock() {
        return new SharedReadLock();
    }

    @Override
    public Lock writeLock() {
        return new SharedWriteLock();
    }

    /**
     * Read view of this {@link SharedReadWriteLock}.
     *
     * @author Jeff Nelson
     */
    class SharedReadLock implements Lock {

        @Override
        public void lock() {
            boolean locked = false;
            while (!locked) {
                writers.writeLock().lock();
                locked = readers.readLock().tryLock();
                writers.writeLock().unlock();
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            boolean locked = false;
            while (!locked) {
                writers.writeLock().lockInterruptibly();
                locked = readers.readLock().tryLock();
                writers.writeLock().unlock();
                if(!locked) {
                    Thread.yield();
                }
            }
        }

        @Override
        public Condition newCondition() {
            return readers.readLock().newCondition();
        }

        @Override
        public boolean tryLock() {
            if(writers.writeLock().tryLock()) {
                try {
                    for (;;) {
                        if(readers.readLock().tryLock()) {
                            return true;
                        }
                        Thread.yield();
                    }
                }
                finally {
                    writers.writeLock().unlock();
                }
            }
            else {
                return false;
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            Stopwatch watch = Stopwatch.createStarted();
            if(writers.writeLock().tryLock(time, unit)) {
                try {
                    for (;;) {
                        watch.stop();
                        long elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        if(readers.readLock().tryLock(time, unit)) {
                            return true;
                        }
                        Thread.yield();
                    }
                }
                finally {
                    writers.writeLock().unlock();
                }
            }
            else {
                return false;
            }
        }

        @Override
        public void unlock() {
            readers.readLock().unlock();
        }

    }

    /**
     * Write view of this {@link SharedReadWriteLock}.
     *
     * @author Jeff Nelson
     */
    class SharedWriteLock implements Lock {

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
        public Condition newCondition() {
            return writers.readLock().newCondition();
        }

        @Override
        public boolean tryLock() {
            if(readers.writeLock().tryLock()) {
                try {
                    for (;;) {
                        if(writers.readLock().tryLock()) {
                            return true;
                        }
                        Thread.yield();
                    }
                }
                finally {
                    readers.writeLock().unlock();
                }
            }
            else {
                return false;
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            Stopwatch watch = Stopwatch.createStarted();
            if(readers.writeLock().tryLock(time, unit)) {
                try {
                    for (;;) {
                        watch.stop();
                        long elapsed = watch.elapsed(unit);
                        time = time - elapsed;
                        watch.start();
                        if(writers.readLock().tryLock(time, unit)) {
                            return true;
                        }
                        Thread.yield();
                    }
                }
                finally {
                    readers.writeLock().unlock();
                }
            }
            else {
                return false;
            }
        }

        @Override
        public void unlock() {
            writers.readLock().unlock();
        }

    }

}