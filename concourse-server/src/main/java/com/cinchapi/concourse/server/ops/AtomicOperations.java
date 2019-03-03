/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.ops;

import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.AtomicSupport;

/**
 * A collection of wrapper functions for executing an {@link AtomicOperation}
 * via an {AtomicRoutine} with special logic (i.e. retry on failure, etc).
 *
 * @author Jeff Nelson
 */
public final class AtomicOperations {

    private AtomicOperations() {/* no-init */}

    /**
     * Run the {@link AtomicRoutine} with an {@link AtomicOperation} from the
     * provided {@code store} and continue to retry execution of the routine
     * until it terminates without failure.
     * 
     * @param store the {@link AtomicSupport} store from which the
     *            {@link AtomicOperation} is started
     * @param routine the {@link AtomicRoutine} to run until it succeeds
     */
    public static void executeWithRetry(AtomicSupport store,
            AtomicRoutine routine) {
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                routine.run(atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

}
