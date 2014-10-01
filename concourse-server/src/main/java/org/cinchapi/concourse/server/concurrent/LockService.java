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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.common.util.NonBlockingHashMultimap;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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

    /**
     * A cache of locks that have been requested. Each Lock is stored as a
     * WeakReference so they are eagerly GCed unless that are active, in which
     * case there is a strong reference to the lock in {@link #refs}.
     */
    private final LoadingCache<Token, TokenReadWriteLock> locks = CacheBuilder
            .newBuilder().softValues()
            .build(new CacheLoader<Token, TokenReadWriteLock>() {

                @Override
                public TokenReadWriteLock load(Token key) throws Exception {
                    return new TokenReadWriteLock(key);
                }

            });

    /**
     * This map holds strong references to TokenReadWriteLocks that are grabbed
     * (e.g. in use). We must keep these strong references while the locks are
     * active so that they are not GCed and we run into monitor state issues.
     */
    private final NonBlockingHashMultimap<Token, TokenReadWriteLockReference> refs = NonBlockingHashMultimap
            .create();

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
        try {
            return locks.get(token).readLock();
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
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
        try {
            return locks.get(token).writeLock();
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

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
        public boolean equals(Object object) {
            if(object instanceof TokenReadWriteLock) {
                TokenReadWriteLock other = (TokenReadWriteLock) object;
                return token.equals(other.token)
                        && getReadLockCount() == other.getReadLockCount()
                        && isWriteLocked() == other.isWriteLocked();
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(token, getReadLockCount(), isWriteLocked());
        }

        @Override
        public ReadLock readLock() {
            return new ReadLock(this) {

                @Override
                public void lock() {
                    super.lock();
                    refs.put(token, new TokenReadWriteLockReference(
                            TokenReadWriteLock.this, Thread.currentThread()));
                }

                @Override
                public void unlock() {
                    super.unlock();
                    refs.remove(token, new TokenReadWriteLockReference(
                            TokenReadWriteLock.this, Thread.currentThread()));
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
                public void lock() {
                    super.lock();
                    refs.put(token, new TokenReadWriteLockReference(
                            TokenReadWriteLock.this, Thread.currentThread()));
                }

                @Override
                public void unlock() {
                    super.unlock();
                    refs.remove(token, new TokenReadWriteLockReference(
                            TokenReadWriteLock.this, Thread.currentThread()));
                }

            };
        }

    }

    /**
     * Holds a reference to a {@link TokenReadWriteLock} that has been grabbed
     * and the thread that grabbed it. This establishes a strong reference to
     * the TokenReadWriteLock, which prevents it from being automatically GCed.
     * 
     * <p>
     * We must associate the locking thread with the lock in order to
     * differentiate the lock event in the {@link #refs} collection.
     * </p>
     * 
     * @author jnelson
     */
    private class TokenReadWriteLockReference {

        /**
         * A strong reference to a TokenReadWriteLock that has been grabbed.
         * This prevents the lock from being GCed and evicted from
         * {@link #locks}.
         */
        private final TokenReadWriteLock lock;

        /**
         * The thread where the {@link #lock} was grabbed.
         */
        private final Thread thread;

        /**
         * Construct a new instance.
         * 
         * @param lock
         * @param thread
         */
        public TokenReadWriteLockReference(TokenReadWriteLock lock,
                Thread thread) {
            this.lock = lock;
            this.thread = thread;
        }

        @Override
        public boolean equals(Object object) {
            if(object instanceof TokenReadWriteLockReference) {
                return lock.equals(((TokenReadWriteLockReference) object).lock)
                        && thread
                                .equals(((TokenReadWriteLockReference) object).thread);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(System.identityHashCode(lock), thread);
        }
    }

    private LockService() {/* noop */}

}
