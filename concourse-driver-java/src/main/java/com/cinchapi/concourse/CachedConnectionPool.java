/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    protected CachedConnectionPool(String host, int port, String username,
            String password, int poolSize) {
        this(host, port, username, password, "", poolSize);
    }

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @param poolSize
     */
    protected CachedConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        super(() -> Concourse.connect(host, port, username, password,
                environment), poolSize);
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
