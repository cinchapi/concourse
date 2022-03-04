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

/**
 * A {@link LockFreeStore} that can initiate and therefore serve as the
 * destination for an {@link AtomicOperation}.
 * <p>
 * {@link AtomicOperation AtomicOperations} use <strong>Just-in-Time
 * Locking</strong>, so initiating {@link LockFreeStore stores} must be
 * able to
 * perform operations without grabbing locks because the {@link AtomicOperation}
 * will do so in bulk prior to {@link AtomicOperation#commit() committing}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public interface AtomicSupport extends
        DurableStore,
        LockFreeStore,
        VersionChangeNotifier {

    /**
     * Return an {@link AtomicOperation} that can be used to group actions that
     * should all succeed or fail together. Use {@link AtomicOperation#commit()}
     * to apply the action to this store or use {@link AtomicOperation#abort()}
     * to cancel.
     * 
     * @return the {@link AtomicOperation}
     */
    public AtomicOperation startAtomicOperation();

}
