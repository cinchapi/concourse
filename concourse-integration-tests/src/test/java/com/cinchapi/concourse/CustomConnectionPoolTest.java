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
package com.cinchapi.concourse;

import java.util.Queue;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.ConcurrentLoadingQueue;

/**
 * Unit tests for custom connection pools.
 *
 * @author Jeff Nelson
 */
public class CustomConnectionPoolTest extends ConnectionPoolTest {

    @Override
    protected ConnectionPool getConnectionPool() {
        return new CustomConnectionPool(SERVER_HOST, SERVER_PORT, USERNAME,
                PASSWORD, 10);
    }

    @Override
    protected ConnectionPool getConnectionPool(String env) {
        return new CustomConnectionPool(SERVER_HOST, SERVER_PORT, USERNAME,
                PASSWORD, env, 10);
    }

    @Test
    public void testConnectionPoolInstantiation() {
        ConnectionPool pool = getConnectionPool();
        Concourse concourse = pool.request();
        try {
            Assert.assertEquals(CustomConcourse.class, concourse.getClass());
        }
        finally {
            pool.release(concourse);
        }
    }

    class CustomConcourse extends ForwardingConcourse {

        /**
         * Construct a new instance.
         * 
         * @param concourse
         */
        public CustomConcourse(Concourse concourse) {
            super(concourse);
        }

        @Override
        protected ForwardingConcourse $this(Concourse concourse) {
            return new CustomConcourse(concourse);
        }

    }

    class CustomConnectionPool extends ConnectionPool {

        // Connection Info
        private final String host;
        private final int port;
        private String username;
        private final String password;
        private final String environment;

        /**
         * Construct a new instance.
         * 
         * @param host
         * @param port
         * @param username
         * @param password
         * @param poolSize
         */
        protected CustomConnectionPool(String host, int port, String username,
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
         * @param poolSize
         */
        protected CustomConnectionPool(String host, int port, String username,
                String password, String environment, int poolSize) {
            super(host, port, username, password, environment, poolSize);
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.environment = environment;
        }

        @Override
        protected Queue<Concourse> buildQueue(int size) {
            return ConcurrentLoadingQueue.create(new Callable<Concourse>() {

                @Override
                public Concourse call() throws Exception {
                    return createConnection(host, port, username, password,
                            environment);
                }

            });
        }

        @Override
        protected Concourse getConnection() {
            return available.poll();
        }

        @Override
        protected Concourse createConnection(String host, int port,
                String username, String password, String environment) {
            return new CustomConcourse(Concourse.connect(host, port, username,
                    password, environment));
        }

    }

}
