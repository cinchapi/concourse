/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.server.concurrent.LockService;
import com.cinchapi.concourse.server.concurrent.RangeLockService;

/**
 * An {@link AtomicOperation} that implements the
 * <a href="https://en.wikipedia.org/wiki/Two-phase_commit_protocol">Two-phase
 * Commit Protocol</a> for distributed operations.
 * <p>
 * Unlike a standard {@link AtomicOperation}, this does not
 * automatically {@link #releaseLocks() release locks} that are grabbed when it
 * is {@link #commit() committed}. To do so, it must be explicitly
 * {@link #finish() finished} (in adherence with its two-phase nature).
 * </p>
 * <p>
 * Since there is no "rollback" for committed {@link AtomicOperation atomic
 * operations}, {@link TwoPhaseCommit} provides data consistency guarantees to
 * participants in a distributed operation by indefinitely blocking resources in
 * the event that the coordinator or a participant is unable to mark a committed
 * transaction as "finished" (in lieu of instructing participants to rollback
 * changes).
 * </p>
 *
 * @author Jeff Nelson
 */
public class TwoPhaseCommit extends AtomicOperation {

    /**
     * Construct a new instance.
     * 
     * @param destination
     * @param lockService
     * @param rangeLockService
     */
    protected TwoPhaseCommit(AtomicSupport destination, LockService lockService,
            RangeLockService rangeLockService) {
        super(destination, lockService, rangeLockService);
    }

    @Override
    protected void releaseLocks() {/* no-op */}

    /**
     * Finish the {@link #commit()} and release all the locks that were grabbed.
     */
    public void finish() {
        if(open.get()) {
            throw new AtomicStateException();
        }
        else {
            super.releaseLocks();
        }
    }

}
