/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

/**
 * A {@link FunctionalInterface} that should be implemented for logic that
 * should run within an {@link AtomicOperation} that may be wrapped in special
 * logic (i.e. retrying the operation on failure).
 *
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface AtomicRoutine {

    /**
     * Execute logic using the provided {@code atomic} operation.
     * 
     * @param atomic the {@link AtomicOperation} to use for interaction with the
     *            underlying data store
     * @throws AtomicStateException
     */
    public void run(AtomicOperation atomic) throws AtomicStateException;

}
