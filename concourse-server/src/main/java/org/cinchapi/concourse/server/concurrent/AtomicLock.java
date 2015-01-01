/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A decorator {@link Lock} that compounds multiple sub locks and operates on
 * them atomically.
 * 
 * @author jnelson
 */
public class AtomicLock implements Lock {

    /**
     * Return an {@link AtomicLock} that compounds and operates on {@code locks}
     * atomically.
     * 
     * @param locks
     * @return the AtomicLock
     */
    public static AtomicLock newInstance(Lock... locks) {
        return new AtomicLock(locks);
    }

    private Lock[] locks;

    /**
     * Construct a new instance.
     * 
     * @param locks
     */
    private AtomicLock(Lock... locks) {
        this.locks = locks;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof AtomicLock) {
            return Arrays.equals(locks, ((AtomicLock) obj).locks);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(locks);
    }

    @Override
    public synchronized void lock() {
        for (Lock lock : locks) {
            lock.lock();
        }
    }

    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        for (Lock lock : locks) {
            lock.lockInterruptibly();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return Arrays.toString(locks);
    }

    @Override
    public synchronized boolean tryLock() {
        for (Lock lock : locks) {
            if(!lock.tryLock()) {
                tryUnlock();
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException {
        for (Lock lock : locks) {
            if(!lock.tryLock(time, unit)) {
                tryUnlock();
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void unlock() {
        for (Lock lock : locks) {
            lock.unlock();
        }
    }

    /**
     * Iterate through and try to unlock each of the {@link #locks}.
     */
    private synchronized void tryUnlock() {
        try {
            for (Lock lock : locks) {
                lock.unlock();
            }
        }
        catch (IllegalMonitorStateException e) {} // --- ignore
    }

}
