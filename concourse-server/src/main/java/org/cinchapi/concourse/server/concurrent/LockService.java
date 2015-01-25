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

import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;

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
public class LockService extends AbstractLockService<Token, TokenReadWriteLock> {

    /**
     * Return a new {@link LockService} instance.
     * 
     * @return the LockService
     */
    public static LockService create() {
        return new LockService(
                new ConcurrentHashMapV8<Token, TokenReadWriteLock>());
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

    private LockService() {/* noop */}

    /**
     * Construct a new instance.
     * 
     * @param locks
     */
    private LockService(ConcurrentHashMapV8<Token, TokenReadWriteLock> locks) {
        super(locks);
    }

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


    @Override
    protected TokenReadWriteLock createLock(Token token) {
        return new TokenReadWriteLock(token);
    }

}
