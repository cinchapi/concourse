/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
import com.cinchapi.ensemble.core.LocalProcess;

/**
 * Can be distributed by the {@link Ensemble} framework.
 *
 * @author Jeff Nelson
 */
public interface Distributed extends AtomicSupport, Ensemble {

    @Override
    public default void $ensembleAbortAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        atomic.abort();
    }

    @Override
    public default void $ensembleFinishCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        atomic.finish();
    }

    @Override
    public default boolean $ensemblePrepareCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        return atomic.commit();
    }

    @Override
    public default EnsembleInstanceIdentifier $ensembleStartAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = new TwoPhaseCommit(identifier, this,
                $ensembleLockBroker());
        LocalProcess.instance().register(atomic); //is this necessary?
        return atomic.$ensembleInstanceIdentifier();
    }

    /**
     * Return the {@link LockBroker} that should be used.
     * 
     * @return the {@link LockBroker}
     */
    public LockBroker $ensembleLockBroker();

}
