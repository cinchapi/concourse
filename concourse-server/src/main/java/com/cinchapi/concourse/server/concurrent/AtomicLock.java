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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A decorator {@link Lock} that compounds multiple sub locks and operates on
 * them atomically.
 * 
 * @author Jeff Nelson
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
