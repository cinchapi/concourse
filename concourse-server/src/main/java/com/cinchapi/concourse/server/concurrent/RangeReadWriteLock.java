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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.concourse.server.model.Value;
import com.google.common.collect.Range;

/**
 * A custom {@link ReentrantReadWriteLock} that is defined by a
 * {@link RangeToken} and checks to see if it is "range" blocked before
 * grabbing a read of write lock.
 * 
 * @author Jeff Nelson
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
    public void afterReadLock() {
        Iterable<Range<Value>> ranges = RangeTokens.convertToRange(token);
        rangeLockService.info.add(token.getKey(), ranges);
    }

    @Override
    public void afterReadUnlock(ReentrantReadWriteLock instance) {
        if(instance.getReadLockCount() == 0) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange(token);
            rangeLockService.info.remove(token.getKey(), ranges);
        }
    }

    @Override
    public void afterWriteUnlock(ReentrantReadWriteLock instance) {
        rangeLockService.info.remove(token.getKey(), token.getValues()[0]);
    }

    @Override
    public void afterWriteLock() {
        rangeLockService.info.add(token.getKey(), token.getValues()[0]);
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
