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
package com.cinchapi.concourse;

import java.util.Queue;
import java.util.function.Supplier;

import com.cinchapi.concourse.util.ConcurrentLoadingQueue;

/**
 * A {@link ConnectionPool} that has no limit on the number of connections it
 * can managed, but will try to use previously requested connections, where
 * possible.
 * 
 * @author Jeff Nelson
 */
class CachedConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance.
     * 
     * @param supplier
     * @param poolSize
     */
    protected CachedConnectionPool(Supplier<Concourse> supplier, int poolSize) {
        super(supplier, poolSize);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return ConcurrentLoadingQueue.create(supplier::get);
    }

    @Override
    protected Concourse getConnection() {
        return available.poll();
    }

}
