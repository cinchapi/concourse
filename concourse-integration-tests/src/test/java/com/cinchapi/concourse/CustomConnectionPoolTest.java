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
package com.cinchapi.concourse;

import java.util.Queue;

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

    static class CustomConcourse extends ForwardingConcourse {

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

    static class CustomConnectionPool extends ConnectionPool {

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
            super(() -> new CustomConcourse(Concourse.connect(host, port,
                    username, password, environment)), poolSize);
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

}
