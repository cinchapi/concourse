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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;

import com.google.common.base.Objects;
import com.google.common.collect.Multiset;

/**
 * A global service that provides ReadLock and WriteLock instances for a given
 * {@link Token}. The locks that are returned from this service can be used to
 * lock <em>notions of things</em> that aren't strictly defined in their own
 * right (i.e. a {@code key} in a {@code record})
 * <p>
 * <strong>WARNING</strong>: If the caller requests a lock for a given token,
 * but does not attempt to grab it immediately, then it is possible that
 * subsequent requests for locks identified by the same token will return
 * different instances. This is unlikely to happen in practice, but it is
 * recommended that lock grabs happen immediately after lock requests just to be
 * safe (e.g <code>LockService.getReadLock(key, record).lock()</code>).
 * </p>
 * 
 * @author jnelson
 */
public class LockService {

    /**
     * Return a new {@link LockService} instance.
     * 
     * @return the LockService
     */
    public static LockService create() {
        return new LockService();
    }

    /**
     * Return a {@link LockService} that does not actually provide any locks.
     * This is
     * used in situations where access is guaranteed (or at least assumed) to be
     * isolated (e.g. a Transaction) and we need to simulate locking for
     * polymorphic consistency.
     * 
     * @return the LockService
     */
    public static LockService noOp() {
        return NOOP_INSTANCE;
    }

    /**
     * A {@link LockService} that does not actually provide any locks. This is
     * used in situations where access is guaranteed (or at least assumed) to be
     * isolated (e.g. a Transaction) and we need to simulate locking for
     * polymorphic consistency.
     */
    private static final LockService NOOP_INSTANCE = new LockService() {

        @Override
        public ReadLock getReadLock(Token token) {
            return Locks.noOpReadLock();
        }

        @Override
        public WriteLock getWriteLock(Token token) {
            return Locks.noOpWriteLock();
        }
    };

    /**
     * A cache of locks that have been requested.
     */
    private final ConcurrentHashMapV8<Token, TokenReadWriteLock> locks = new ConcurrentHashMapV8<Token, TokenReadWriteLock>();

    private LockService() {/* noop */}

    /**
     * Return the ReadLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the ReadLock
     */
    public ReadLock getReadLock(Object... objects) {
        return getReadLock(Token.wrap(objects));
    }

    /**
     * Return the ReadLock that is identified by {@code token}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param token
     * @return the ReadLock
     */
    public ReadLock getReadLock(Token token) {
        Thread thread = Thread.currentThread();
        TokenReadWriteLock existing = locks.get(token);
        if(existing == null) {
            TokenReadWriteLock created = new TokenReadWriteLock(token);
            existing = locks.putIfAbsent(token, created);
            existing = Objects.firstNonNull(existing, created);
        }
        existing.readers.setCount(thread, 1);
        return locks.putIfAbsent(token, existing) == existing ? existing
                .readLock() : getReadLock(token);
    }

    /**
     * Return the WriteLock that is identified by {@code objects}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param objects
     * @return the WriteLock
     */
    public WriteLock getWriteLock(Object... objects) {
        return getWriteLock(Token.wrap(objects));
    }

    /**
     * Return the WriteLock that is identified by {@code token}. Every caller
     * requesting a lock for {@code token} is guaranteed to get the same
     * instance if the lock is currently held by a reader of a writer.
     * 
     * @param token
     * @return the WriteLock
     */
    public WriteLock getWriteLock(Token token) {
        Thread thread = Thread.currentThread();
        TokenReadWriteLock existing = locks.get(token);
        if(existing == null) {
            TokenReadWriteLock created = new TokenReadWriteLock(token);
            existing = locks.putIfAbsent(token, created);
            existing = Objects.firstNonNull(existing, created);
        }
        existing.writers.setCount(thread, 1);
        return locks.putIfAbsent(token, existing) == existing ? existing
                .writeLock() : getWriteLock(token);
    }

    /**
     * A custom {@link ReentrantReadWriteLock} that is defined by a
     * {@link Token}.
     * 
     * @author jnelson
     */
    @SuppressWarnings("serial")
    private final class TokenReadWriteLock extends ReentrantReadWriteLock {

        /**
         * We keep track of all the threads that have requested (but not
         * necessarily locked) the read or write lock. If a lock is not
         * associated with any threads then it can be safely removed from the
         * cache.
         */
        private final Multiset<Thread> readers = GuavaInternals
                .newSynchronizedHashMultiset();

        /**
         * We keep track of all the threads that have requested (but not
         * necessarily locked) the read or write lock. If a lock is not
         * associated with any threads then it can be safely removed from the
         * cache.
         */
        private final Multiset<Thread> writers = GuavaInternals
                .newSynchronizedHashMultiset();

        /**
         * The token that represents the notion this lock controls
         */
        private final Token token;

        /**
         * Construct a new instance.
         * 
         * @param token
         */
        public TokenReadWriteLock(Token token) {
            this.token = token;
        }

        @Override
        public boolean equals(Object object) {
            // This is a BAD implementation for equality, but for our purposes
            // we only care to see that the tokens are the same and there are no
            // readers or writers.
            if(object instanceof TokenReadWriteLock) {
                TokenReadWriteLock other = (TokenReadWriteLock) object;
                return token.equals(other.token)
                        && readers.size() == other.readers.size()
                        && writers.size() == other.writers.size();
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(token, readers.size(), writers.size());
        }

        @Override
        public ReadLock readLock() {
            return new ReadLock(this) {

                @Override
                public void unlock() {
                    super.unlock();
                    readers.remove(Thread.currentThread(), 1);
                    locks.remove(token, new TokenReadWriteLock(token));
                }

            };
        }

        @Override
        public String toString() {
            return super.toString() + "[id = " + System.identityHashCode(this)
                    + "]";
        }

        @Override
        public WriteLock writeLock() {
            return new WriteLock(this) {

                @Override
                public void unlock() {
                    super.unlock();
                    writers.remove(Thread.currentThread(), 1);
                    locks.remove(token, new TokenReadWriteLock(token));
                }

            };
        }

    }

}
