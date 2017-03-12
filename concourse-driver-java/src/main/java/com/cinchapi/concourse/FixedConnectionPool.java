/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import com.google.common.base.Throwables;

/**
 * A {@link ConnectionPool} with a fixed number of connections. If all the
 * connections from the pool are active, subsequent request attempts will block
 * until a connection is returned.
 * 
 * @author Jeff Nelson
 */
class FixedConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    protected FixedConnectionPool(String host, int port, String username,
            String password, int poolSize) {
        super(host, port, username, password, poolSize);
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
    protected FixedConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        super(host, port, username, password, environment, poolSize);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return new ArrayBlockingQueue<Concourse>(size);
    }

    @Override
    protected Concourse getConnection() {
        try {
            return ((BlockingQueue<Concourse>) available).take();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

}
