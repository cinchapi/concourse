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
package com.cinchapi.concourse.server.ops;

import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;

/**
 * An {@link AtomicSupplier} is an {@link AtomicRoutine} that returns a value.
 * <p>
 * The {@link AtomicSupplier} uses an {@link AtomicOperation} to supply a value.
 * </p>
 *
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface AtomicSupplier<T> {

    /**
     * Given the {@link AtomicOperation atomic} operation, supply a value.
     * 
     * @param atomic
     * @return the supplied value
     * @throws AtomicStateException
     */
    public T supply(AtomicOperation atomic) throws AtomicStateException;

}
