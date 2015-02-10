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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A custom {@link ReentrantReadWriteLock} that is defined by a
 * {@link RangeToken} and checks to see if it is "range" blocked before
 * grabbing a read of write lock.
 * 
 * @author jnelson
 */
@SuppressWarnings("serial")
final class RangeReadWriteLock extends ReferenceCountingLock {

    /**
     * The reference to the {@link RangeLockService} that has information about
     * other range locks in scope, so this lock can detect if it is range
     * blocked or not.
     */
    private final RangeLockService rangeLockService;

    /**
     * The token that represents the notion this lock controls
     */
    private final RangeToken token;

    /**
     * Construct a new instance.
     * 
     * @param rangeLockService
     * @param token
     */
    public RangeReadWriteLock(RangeLockService rangeLockService,
            RangeToken token) {
        super(new ReentrantReadWriteLock());
        this.rangeLockService = rangeLockService;
        this.token = token;
    }

    @Override
    public void beforeReadLock() {
        while (rangeLockService.isRangeBlocked(LockType.READ, token)) {
            Thread.yield();
            continue;
        }
    }

    @Override
    public boolean tryBeforeReadLock() {
        return !rangeLockService.isRangeBlocked(LockType.READ, token);
    }


    @Override
    public void beforeWriteLock() {
        while (rangeLockService.isRangeBlocked(LockType.WRITE, token)) {
            Thread.yield();
            continue;
        }
    }

    @Override
    public boolean tryBeforeWriteLock() {
        return !rangeLockService.isRangeBlocked(LockType.WRITE, token);
    }

}