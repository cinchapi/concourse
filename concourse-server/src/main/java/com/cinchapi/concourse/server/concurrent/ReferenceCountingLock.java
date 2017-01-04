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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.concourse.annotate.PackagePrivate;

/**
 * A decorated {@link ReentrantReadWriteLock} that has a counter to keep track
 * of references. References are decremented when the read or write lock is
 * released. The subclass should increment the reference count when the lock is
 * considered to be <em>grabbed</em>
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
@SuppressWarnings("serial")
class ReferenceCountingLock extends ReentrantReadWriteLock {

    // NOTE: This class does not define hashCode() or equals() because the
    // defaults are the desired behaviour

    @Override
    public int getReadLockCount() {
        return decorated.getReadLockCount();
    }

    @Override
    public boolean isWriteLocked() {
        return decorated.isWriteLocked();
    }

    @Override
    public boolean isWriteLockedByCurrentThread() {
        return decorated.isWriteLockedByCurrentThread();
    }

    @Override
    public int getWriteHoldCount() {
        return decorated.getWriteHoldCount();
    }

    @Override
    public int getReadHoldCount() {
        return decorated.getReadHoldCount();
    }

    @Override
    public boolean hasWaiters(Condition condition) {
        return decorated.hasWaiters(condition);
    }

    @Override
    public int getWaitQueueLength(Condition condition) {
        return decorated.getWaitQueueLength(condition);
    }

    /**
     * A counter that keeps track of "references" to this lock. Each time
     * the lock is requested, the counter is incremented. Each time the lock
     * is released, the counter is decremented. This counter helps to
     * perform periodic cleanup.
     */
    @PackagePrivate
    final AtomicInteger refs = new AtomicInteger(0);

    /**
     * The lock that is decorated by this reference counting wrapper.
     */
    private final ReentrantReadWriteLock decorated;

    /**
     * A reference to the readLock that performs the work in the
     * {@link #readLock()} method.
     */
    private final ReadLock readLock0;

    /**
     * A reference to the writeLock that performs the work in the
     * {@link #writeLock()} method.
     */
    private final WriteLock writeLock0;

    /**
     * The {@link ReadLock} instance that is returned from the
     * {@link #readLock()} method.
     */
    private final ReadLock readLock;

    /**
     * The {@link WriteLock} instance that is returned from the
     * {@link #writeLock()} method.
     */
    private final WriteLock writeLock;

    /**
     * Construct a new instance.
     * 
     * @param decorated
     */
    ReferenceCountingLock(ReentrantReadWriteLock decorated) {
        this.decorated = decorated;
        this.readLock0 = decorated.readLock();
        this.writeLock0 = decorated.writeLock();

        this.readLock = new DecoratedReadLock();
        this.writeLock = new DecoratedWriteLock();
    }

    /**
     * A {@link WriteLock} that handles reference counting.
     * 
     * @author Jeff Nelson
     */
    private class DecoratedWriteLock extends WriteLock {

        /**
         * Construct a new instance.
         */
        protected DecoratedWriteLock() {
            super(decorated);
        }

        @Override
        public void lock() {
            beforeWriteLock();
            writeLock0.lock();
            afterWriteLock();
        }

        @Override
        public boolean tryLock() {
            if(tryBeforeWriteLock() && writeLock0.tryLock()) {
                afterWriteLock();
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public void unlock() {
            writeLock0.unlock();
            afterWriteUnlock(decorated);
            refs.decrementAndGet();    
        }

    }

    /**
     * A {@link ReadLock} that handles reference counting.
     * 
     * @author Jeff Nelson
     */
    private class DecoratedReadLock extends ReadLock {

        /**
         * Construct a new instance.
         */
        protected DecoratedReadLock() {
            super(decorated);
        }

        @Override
        public void lock() {
            beforeReadLock();
            readLock0.lock();
            afterReadLock();
        }

        @Override
        public boolean tryLock() {
            if(tryBeforeReadLock() && readLock0.tryLock()) {
                afterReadLock();
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public void unlock() {
            readLock0.unlock();
            afterReadUnlock(decorated);
            refs.decrementAndGet();
            
        }

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
     * This (optional) method is always run before grabbing the read lock. It is
     * useful for cases where it is necessary to check some additional state
     * before proceeding to the locking routing.
     */
    protected void beforeReadLock() {/* noop */}

    /**
     * This (optional) method is always run after grabbing the read lock. It is
     * useful for cases where it is necessary to update some additional state.
     */
    protected void afterReadLock() {/* noop */}

    /**
     * This (optional) method is always run after releasing the read lock. It is
     * useful for cases where it is necessary to cleanup state.
     * 
     * @param instance
     */
    protected void afterReadUnlock(ReentrantReadWriteLock instance) {/* noop */}

    /**
     * This (optional) method is always run before grabbing the write lock. It
     * is useful for cases where it is necessary to check some additional state
     * before proceeding to the locking routing.
     */
    protected void beforeWriteLock() {/* noop */}

    /**
     * This (optional) method is always run after grabbing the write lock. It is
     * useful for cases where it is necessary to update some additional state.
     */
    protected void afterWriteLock() {/* noop */}

    /**
     * This (optional) method is always run after releasing the write lock. It
     * is useful for cases where it is necessary to cleanup state.
     * 
     * @param instance
     */
    protected void afterWriteUnlock(ReentrantReadWriteLock instance) {/* noop */}

    /**
     * This (optional) method is always run before an attempt is made to grab
     * the read lock. It is useful for cases where it is necessary to check some
     * additional state to determine if the read lock is grabable.
     * 
     * @return {@code true} if the routine determines an attempt should be made
     *         to grab the read lock
     */
    protected boolean tryBeforeReadLock() {
        return true;
    }

    /**
     * This (optional) method is always run before an attempt is made to grab
     * the write lock. It is useful for cases where it is necessary to check
     * some
     * additional state to determine if the write lock is grabable.
     * 
     * @return {@code true} if the routine determines an attempt should be made
     *         to grab the write lock
     */
    protected boolean tryBeforeWriteLock() {
        return true;
    }
}
