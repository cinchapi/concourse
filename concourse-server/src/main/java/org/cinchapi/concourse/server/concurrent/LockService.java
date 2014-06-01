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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.util.Numbers;

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
public final class LockService {

    /**
     * Return a new {@link LockService} instance.
     * 
     * @return the LockService
     */
    public static LockService create() {
        return new LockService();
    }

    private final ReentrantLock lock = new ReentrantLock();

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
        lock.lock();
        try {
            refs.get(token).incrementAndGet();
            return cache.get(token).readLock();
        }
        finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            refs.get(token).incrementAndGet();
            return cache.get(token).writeLock();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * This is cache that is responsible for returning the same lock
     * instance for a given token. This cache will periodically evict lock
     * instances that are not currently held by any readers or writers.
     */
    @SuppressWarnings("serial")
    private final Map<Token, TokenReadWriteLock> cache = new ConcurrentHashMap<Token, TokenReadWriteLock>() {

        @Override
        public TokenReadWriteLock get(Object key) {
            if(!containsKey(key)) {
                Token token = (Token) key;
                TokenReadWriteLock lock = new TokenReadWriteLock(token);
                put(token, lock);
                return lock;
            }
            return super.get(key);
        }

    };

    /**
     * The running number of references to a lock instance associated with a
     * given {@link Token}. We use reference counting to track when a lock for a
     * given token is requested by a thread.
     */
    @SuppressWarnings("serial")
    private final Map<Token, AtomicInteger> refs = new ConcurrentHashMap<Token, AtomicInteger>() {
        @Override
        public AtomicInteger get(Object key) {
            if(!containsKey(key)) {
                AtomicInteger integer = new AtomicInteger(0);
                put((Token) key, integer);
                return integer;
            }
            return super.get(key);
        }
    };

    /**
     * A custom {@link ReentrantReadWriteLock} that is defined by a
     * {@link Token}.
     * 
     * @author jnelson
     */
    @SuppressWarnings("serial")
    private final class TokenReadWriteLock extends ReentrantReadWriteLock {

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
        public ReadLock readLock() {
            return new ReadLock(this) {

                @Override
                public void unlock() {
                    super.unlock();
                    lock.lock();
                    try {
                        if(Numbers.isEven(refs.get(token).get())
                                && !TokenReadWriteLock.this.isWriteLocked()
                                && TokenReadWriteLock.this.getReadLockCount() == 0) {
                            cache.remove(token);
                            refs.remove(token);
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

            };
        }

        @Override
        public WriteLock writeLock() {
            return new WriteLock(this) {

                @Override
                public void unlock() {
                    super.unlock();
                    lock.lock();
                    try {
                        if(Numbers.isEven(refs.get(token).get())
                                && !TokenReadWriteLock.this.isWriteLocked()
                                && TokenReadWriteLock.this.getReadLockCount() == 0) {
                            cache.remove(token);
                            refs.remove(token);
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

            };
        }

    }

    private LockService() {}

}
