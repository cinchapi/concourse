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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An extension of a {@link ReentrantReadWriteLock} that has a preference for
 * either readers or writers.
 * <p>
 * A {@link PriorityReadWriteLock} is useful in cases when there is a desire to
 * prefer readers over writers or vice versa under contention. Prioritized
 * actors will try to grab their locks immediately whereas unprivileged ones
 * will deferentially linger for a while before attempting to grab theirs. This
 * means that a prioritized actor will always grab her lock before an
 * unprioritized one who tries to grab his at the same time.
 * </p>
 * <p>
 * These locks have logic to ensure that unprioritized actors only defer to
 * prioritized ones in the event that there is contention for the lock. If there
 * is none, then unprioritized actors will generally grab their locks
 * immediately.
 * </p>
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class PriorityReadWriteLock extends ReentrantReadWriteLock {

    /**
     * Return a {@link PriorityReadWriteLock} that has a preference for
     * readers over writers under contention.
     * 
     * @return the lock
     */
    public static PriorityReadWriteLock prioritizeReads() {
        return new PriorityReadWriteLock(true);
    }

    /**
     * Return a {@link PriorityReadWriteLock} that has a preference for
     * writers over readers under contention.
     * 
     * @return the lock
     */
    public static PriorityReadWriteLock prioritizeWrites() {
        return new PriorityReadWriteLock(false);
    }

    /**
     * A flag that indicates that unprivileged actor must spin before trying to
     * grab the lock. By default, this is set to {@code false}, but a privileged
     * actor will set this to {@code true} when they try to grab the lock.
     * Conversely, unprivileged actors always set this to {@code false}, which
     * means that they generally won't ever spin while there aren't any lock
     * attempts from privileged actors.
     */
    private volatile boolean forceSpin = false;

    /**
     * The lock that is returned from the {@link #readLock()} method.
     */
    private final ReadLock readLock;

    /**
     * The lock that is returned from the {@link #writeLock()} method.
     */
    private final WriteLock writeLock;

    /**
     * Construct a new instance. If {@code privilegedReads} is {@code true},
     * then the resulting lock with be biased towards read locking and biased
     * against write locking. The opposite is true if the parameter is
     * {@code false}.
     * 
     * @param prioritizeReads
     */
    private PriorityReadWriteLock(boolean prioritizeReads) {
        readLock = prioritizeReads ? new PriorityReadLock(this)
                : new UnpriorityReadLock(this);
        writeLock = prioritizeReads ? new UnpriorityWriteLock(this)
                : new PriorityWriteLock(this);
    }

    @Override
    public ReadLock readLock() {
        return readLock;
    }

    @Override
    public WriteLock writeLock() {
        return writeLock;
    }

    /**
     * Spin if it is necessary to do so.
     */
    private void trySpin() {
        if(forceSpin) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * An {@link ReadLock} that does not defer to writers.
     * 
     * @author Jeff Nelson
     */
    private final class PriorityReadLock extends ReadLock {

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        protected PriorityReadLock(PriorityReadWriteLock lock) {
            super(lock);
        }

        @Override
        public void lock() {
            forceSpin = true;
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            forceSpin = true;
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            forceSpin = true;
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException {
            forceSpin = true;
            return super.tryLock(timeout, unit);
        }

    }

    /**
     * An {@link WriteLock} that does not defer to readers.
     * 
     * @author Jeff Nelson
     */
    private final class PriorityWriteLock extends WriteLock {

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        protected PriorityWriteLock(PriorityReadWriteLock lock) {
            super(lock);
        }

        @Override
        public void lock() {
            forceSpin = true;
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            forceSpin = true;
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            forceSpin = true;
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException {
            forceSpin = true;
            return super.tryLock(timeout, unit);
        }
    }

    /**
     * An {@link ReadLock} that defers to writers under contention.
     * 
     * @author Jeff Nelson
     */
    private final class UnpriorityReadLock extends ReadLock {

        /**
         * Construct a new instance.
         * 
         * @param lock
         */
        protected UnpriorityReadLock(PriorityReadWriteLock lock) {
            super(lock);
        }

        @Override
        public void lock() {
            trySpin();
            super.lock();
            forceSpin = false;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            trySpin();
            super.lockInterruptibly();
            forceSpin = false;
        }

        @Override
        public boolean tryLock() {
            trySpin();
            forceSpin = false;
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException {
            trySpin();
            if(super.tryLock(timeout, unit)) {
                forceSpin = false;
                return true;
            }
            else {
                return false;
            }
        }

    }

    /**
     * An {@link WriteLock} that defers to readers under contention.
     * 
     * @author Jeff Nelson
     */
    private final class UnpriorityWriteLock extends WriteLock {

        /**
         * Constructa new instance.
         * 
         * @param lock
         */
        protected UnpriorityWriteLock(PriorityReadWriteLock lock) {
            super(lock);
        }

        @Override
        public void lock() {
            trySpin();
            super.lock();
            forceSpin = false;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            trySpin();
            super.lockInterruptibly();
            forceSpin = false;
        }

        @Override
        public boolean tryLock() {
            trySpin();
            forceSpin = false;
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException {
            trySpin();
            if(super.tryLock(timeout, unit)) {
                forceSpin = false;
                return true;
            }
            else {
                return false;
            }
        }

    }

}
