/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
import com.cinchapi.ensemble.Ensemble;
import com.cinchapi.ensemble.EnsembleInstanceIdentifier;

/**
 * An {@link AtomicOperation} that implements the
 * <a href="https://en.wikipedia.org/wiki/Two-phase_commit_protocol">Two-phase
 * Commit Protocol</a> for distributed operations.
 * <p>
 * Unlike a standard {@link AtomicOperation}, this does not apply operations
 * when it is {@link #commit() committed}. Instead it only grabs locks. To apply
 * the operations, it must be explicitly {@link #finish() finished} (in
 * adherence with its two-phase nature).
 * </p>
 * <p>
 * Since there is no "rollback" for committed {@link AtomicOperation atomic
 * operations}, {@link TwoPhaseCommit} provides data consistency guarantees to
 * participants in a distributed operation by indefinitely blocking resources in
 * the event that the coordinator or a participant is unable to mark a committed
 * transaction as "finished" (in lieu of instructing participants to rollback
 * changes).
 * </p>
 * <p>
 * This construct is necessary to support distributed atomic operations using
 * the {@link Ensemble} framework.
 * </p>
 *
 * @author Jeff Nelson
 */
class TwoPhaseCommit extends AtomicOperation {

    /**
     * The {@link EnsembleInstanceIdentifier} that is assigned when
     * {@link Ensemble#$ensembleStartAtomic(EnsembleInstanceIdentifier)}.
     */
    private final EnsembleInstanceIdentifier identifier;

    /**
     * The version to assign when {@link #finish() finishing the commit}.
     */
    private Long version = null;

    /**
     * Construct a new instance.
     * 
     * @param destination
     * @param lockService
     * @param rangeLockService
     */
    protected TwoPhaseCommit(EnsembleInstanceIdentifier identifier,
            AtomicSupport destination, LockBroker broker) {
        super(destination, broker);
        this.identifier = identifier;
    }

    @Override
    public void $ensembleAbortAtomic(EnsembleInstanceIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void $ensembleFinishCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EnsembleInstanceIdentifier $ensembleInstanceIdentifier() {
        return identifier;
    }

    @Override
    public boolean $ensemblePrepareCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        return false;
    }

    @Override
    public EnsembleInstanceIdentifier $ensembleStartAtomic(
            EnsembleInstanceIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort() {
        super.cancel();
    }

    /**
     * Finish the {@link #commit()} and release all the locks that were grabbed.
     */
    public void finish() {
        if(version != null) {
            super.complete(version);
        }
        else {
            throwAtomicStateException();
        }
    }

    @Override
    protected void complete(long version) {
        this.version = version;
        // Don't actually perform the completion work. This ensures that the
        // only thing that happens when #commit() is called is that the locks
        // are acquired. The actual completion work happens when #finish() is
        // called.
    }

}