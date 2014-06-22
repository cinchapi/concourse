/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A {@link ConnectionPool} that has no limit on the number of connections it
 * can managed, but will try to use previously requested connections, where
 * possible.
 * 
 * @author jnelson
 */
class CachedConnectionPool extends ConnectionPool {

    // Each connection will, if not active, will automatically expire. These
    // units are chosen to correspond to the AccessToken TTL that is defined in
    // {@link AccessManager}.
    private static final int CONNECTION_TTL = 24;
    private static final TimeUnit CONNECTION_TTL_UNIT = TimeUnit.HOURS;

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
        super(host, port, username, password, environment, poolSize);
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.environment = environment;
    }

    @Override
    public boolean hasAvailableConnection() {
        return true;
    }

    @Override
    public Concourse request() {
        try {
            return super.request();
        }
        catch (IllegalStateException e) {
            Concourse connection = Concourse.connect(host, port, username,
                    password, environment);
            connections.put(connection, new AtomicBoolean(true));
            return connection;
        }
    }

    @Override
    protected Cache<Concourse, AtomicBoolean> buildCache(int size) {
        return CacheBuilder
                .newBuilder()
                .initialCapacity(size)
                .expireAfterWrite(CONNECTION_TTL, CONNECTION_TTL_UNIT)
                .removalListener(
                        new RemovalListener<Concourse, AtomicBoolean>() {

                            @Override
                            public void onRemoval(
                                    RemovalNotification<Concourse, AtomicBoolean> notification) {
                                if(notification.getValue().get()) { // ensure
                                                                    // that
                                                                    // active
                                                                    // connections
                                                                    // don't get
                                                                    // dropped
                                    connections.put(notification.getKey(),
                                            notification.getValue());
                                }
                            }

                        }).build();
    }

}
