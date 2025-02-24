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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.util.Logger;
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
     * Return the {@link Allocator} that manages {@link TwoPhaseCommit}
     * instances.
     * 
     * @return the {@link Allocator}
     */
    public static Allocator allocator() {
        return ALLOCATOR;
    }

    /**
     * The canonical {@link #allocator()}.
     */
    private static final Allocator ALLOCATOR = new Allocator();

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
    TwoPhaseCommit(EnsembleInstanceIdentifier identifier,
            AtomicSupport destination, LockBroker broker) {
        super(destination, broker, null);
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
    public <T> T $ensembleInvokeAtomic(EnsembleInstanceIdentifier identifier,
            String method, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ensemble $ensembleLocateAtomicInstance(
            EnsembleInstanceIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean $ensemblePrepareCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        return false;
    }

    @Override
    public void $ensembleStartAtomic(EnsembleInstanceIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort() {
        super.cancel();
        Logger.debug("Canceled two phase commit {}", this);
    }

    /**
     * Finish the {@link #commit()} and release all the locks that were grabbed.
     */
    public void finish() {
        if(version != null) {
            super.complete(version);
            Logger.debug("Finished two phase commit {}", this);
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
        Logger.debug("Completed two phase commit {} at version {}", this,
                version);
    }

    /**
     * Deallocate this {@link TwoPhaseCommit}.
     * 
     * @return {@code true} if this is deallocated
     */
    boolean deallocate() {
        return allocator().deallocate((AtomicSupport) durable, identifier);
    }

    /**
     * Provides methods to control the lifecycle of {@link TwoPhaseCommit}
     * instances.
     *
     * @author Jeff Nelson
     */
    static class Allocator {

        /**
         * The {@link TwoPhaseCommit TwoPhaseCommits} that are currently
         * {@link #allocate(EnsembleInstanceIdentifier, AtomicSupport, LockBroker)
         * allocated}.
         * 
         * @implNote It is assumed that assigned EnsembleInstanceIdentifiers are
         *           unique across destinations, so those are not maintained
         *           within the map.
         */
        private final Map<EnsembleInstanceIdentifier, TwoPhaseCommit> commits = new ConcurrentHashMap<>();

        /**
         * Construct a new instance.
         */
        private Allocator() { /* no init */}

        /**
         * Allocate a new {@link TwoPhaseCommit} that is
         * {@link #get(AtomicSupport, EnsembleInstanceIdentifier) retrievable}
         * with {@code identifier} and acts as a sandbox for the
         * {@code destination} using the {@code broker}.
         * 
         * @param identifier
         * @param destination
         * @param broker
         * @return the allocated {@link TwoPhaseCommit}
         */
        public TwoPhaseCommit allocate(EnsembleInstanceIdentifier identifier,
                AtomicSupport destination, LockBroker broker) {
            return commits.computeIfAbsent(identifier,
                    $ -> new TwoPhaseCommit($, destination, broker));
        }

        /**
         * Deallocate the {@link TwoPhaseCommit}.
         * 
         * @param destination
         * @param identifier
         * @return a boolean that indicates if the {@link TwoPhaseCommit} was
         *         deallocated
         */
        public boolean deallocate(AtomicSupport destination,
                EnsembleInstanceIdentifier identifier) {
            return commits.remove(identifier) != null;
        }

        /**
         * Return the allocated {@link TwoPhaseCommit} with {@code identifier}
         * that is an extension of {@code destination}.
         * 
         * @param destination
         * @param identifier
         * @return the {@link TwoPhaseCommit} or {@code null} if it does not
         *         exist.
         */
        @Nullable
        public TwoPhaseCommit get(AtomicSupport destination,
                EnsembleInstanceIdentifier identifier) {
            return commits.get(identifier);
        }

    }

}
