/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.storage.AtomicOperation.Status;
import com.cinchapi.concourse.time.TimeSource;
import com.cinchapi.ensemble.Ensemble;
import com.cinchapi.ensemble.EnsembleInstanceIdentifier;

/**
 * Can be distributed by the {@link Ensemble} framework.
 * <p>
 * This interface provides default adherence to {@link Ensemble Ensemble's}
 * two-phase commit protocol via the {@link TwoPhaseCommit} construct. This
 * interface automatically manages the registration and lifecycle of
 * {@link TwoPhaseCommit} instances so that they can be operated on by
 * individual nodes within an {@link Ensemble} cluster.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Distributed extends AtomicSupport, Ensemble {

    @Override
    public default void $ensembleAbortAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = TwoPhaseCommit.allocator().get(this,
                identifier);
        try {
            atomic.abort();
        }
        finally {
            atomic.deallocate();
        }
    }

    @Override
    public default void $ensembleFinishCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = TwoPhaseCommit.allocator().get(this,
                identifier);
        try {
            atomic.finish();
        }
        finally {
            atomic.deallocate();
        }
    }

    /**
     * Return the {@link LockBroker} that should be used.
     * 
     * @return the {@link LockBroker}
     */
    public LockBroker $ensembleLockBroker();

    @Override
    public default boolean $ensemblePrepareCommitAtomic(
            EnsembleInstanceIdentifier identifier, long timestamp) {
        TwoPhaseCommit atomic = TwoPhaseCommit.allocator().get(this,
                identifier);
        if(atomic.status() == Status.FINALIZING) {
            // In cases where an Ensemble Tandem includes multiple Cohorts with
            // the same leader node, the #2pc will be committed more than once.
            return true;
        }
        else {
            timestamp = TimeSource.get().interpret(timestamp);
            return atomic.commit(timestamp);
        }
    }

    @Override
    public default void $ensembleStartAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit.allocator().allocate(identifier, this,
                $ensembleLockBroker());
    }

    @Override
    default Ensemble $ensembleLocateAtomicInstance(
            EnsembleInstanceIdentifier identifier) {
        return TwoPhaseCommit.allocator().get(this, identifier);
    }

}
