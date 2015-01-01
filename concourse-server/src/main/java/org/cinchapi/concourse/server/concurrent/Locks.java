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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Lock related utility methods.
 * 
 * @author jnelson
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
