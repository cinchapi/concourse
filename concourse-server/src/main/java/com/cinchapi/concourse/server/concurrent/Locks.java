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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Lock related utility methods.
 * 
 * @author Jeff Nelson
 */
public final class Locks {

    /**
     * Call {@link Lock#lock()} if and only if {@code condition} is {@code true}
     * 
     * @param lock
     * @param condition
     */
    public static void lockIfCondition(Lock lock, boolean condition) {
        if(condition) {
            lock.lock();
        }
    }

    /**
     * Return a {@link ReadLock} that is non-operational and always returns
     * immediately without actually acquiring any shared or exclusive holds on
     * any monitor.
     * 
     * @return the noop ReadLock
     */
    public static ReadLock noOpReadLock() {
        return NOOP_READ_LOCK;
    }

    /**
     * Return a {@link StampedLock} that is non-operational and always returns
     * immediately without actually acquiring and shard or exclusive holds on
     * any monitor.
     * 
     * @return the noop StampedLock
     */
    public static StampedLock noOpStampedLock() {
        return NOOP_STAMPED_LOCK;
    }

    /**
     * Return a {@link WriteLock} that is non-operational and always returns
     * immediately without actually acquiring any shared or exclusive holds on
     * any monitor.
     * 
     * @return the noop WriteLock
     */
    public static WriteLock noOpWriteLock() {
        return NOOP_WRITE_LOCK;
    }

    /**
     * Decorator to call {@link StampedLock#readLock()} if the {@code condition}
     * is {@code true}.
     * 
     * @param lock
     * @param condition
     * @return the stamp
     */
    public static long stampLockReadIfCondition(StampedLock lock,
            boolean condition) {
        return condition ? lock.readLock() : 0L;
    }

    /**
     * Decorator to call {@link StampedLock#writeLock()} if the
     * {@code condition} is {@code true}.
     * 
     * @param lock
     * @param condition
     * @return the stamp
     */
    public static long stampLockWriteIfCondition(StampedLock lock,
            boolean condition) {
        return condition ? lock.writeLock() : 0L;
    }

    /**
     * Decorator to call {@link StampedLock#unlockRead(long)} if the
     * {@code condition} is {@code true}.
     * 
     * @param lock
     * @param condition
     */
    public static void stampUnlockReadIfCondition(StampedLock lock, long stamp,
            boolean condition) {
        if(condition) {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Decorator to call {@link StampedLock#unlockWrite(long)} if the
     * {@code condition} is {@code true}.
     * 
     * @param lock
     * @param condition
     */
    public static void stampUnlockWriteIfCondition(StampedLock lock,
            long stamp, boolean condition) {
        if(condition) {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Call {@link Lock#unlock()} if and only if {@code condition} is
     * {@code true}. This method DOES NOT check to see if the lock is actually
     * held.
     * 
     * @param lock
     * @param condition
     */
    public static void unlockIfCondition(Lock lock, boolean condition) {
        if(condition) {
            lock.unlock();
        }
    }

    /**
     * The lock that is returned by the {@link #noOpReadLock()} method.
     */
    @SuppressWarnings("serial")
    private static final ReadLock NOOP_READ_LOCK = new ReadLock(
            new ReentrantReadWriteLock()) {
        @Override
        public void lock() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void unlock() {}
    };

    /**
     * A {@link StampedLock} that does not do anything. This is returned by the
     * {@link #noOpStampedLock()} method.
     */
    @SuppressWarnings("serial")
    private static final StampedLock NOOP_STAMPED_LOCK = new StampedLock() {

        @Override
        public long readLock() {
            return 0;
        }

        @Override
        public long tryOptimisticRead() {
            return 0;
        }

        @Override
        public void unlock(long stamp) {/* noop */}

        @Override
        public boolean validate(long stamp) {
            return true;
        }

        @Override
        public long writeLock() {
            return 0;
        }

    };

    /**
     * The lock that is returned by the {@link #noOpWriteLock()} method.
     */
    @SuppressWarnings("serial")
    private static final WriteLock NOOP_WRITE_LOCK = new WriteLock(
            new ReentrantReadWriteLock()) {
        @Override
        public void lock() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void unlock() {}
    };

}
