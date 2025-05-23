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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.config.ConcourseClientConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * <p>
 * A {@link ConnectionPool} manages multiple concurrent connections to Concourse
 * for a single user. Generally speaking, a ConnectionPool is handy in long
 * running server API endpoints that want to reduce the overhead of creating a
 * new client connection for every asynchronous request. Using a ConnectionPool,
 * those applications can maintain a finite number of connections while ensuring
 * the resources are disconnected gracefully when necessary.
 * </p>
 * <h2>Usage</h2>
 * 
 * <pre>
 * Concourse concourse = pool.request();
 * try {
 *  ...
 * }
 * finally {
 *  pool.release(concourse);
 * }
 * ...
 * // All the threads with connections are done
 * pool.close()
 * </pre>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public abstract class ConnectionPool implements AutoCloseable {

    // NOTE: This class does not define #hashCode or #equals because the
    // defaults are the desired behaviour

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance described in the
     * {@code concourse_client.prefs} file located in the working directory or
     * the default connection info if no such file exists, but will try to use
     * previously created connections before establishing new ones for any
     * request.
     * 
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool() {
        return newCachedConnectionPool(DEFAULT_CONFIG_FILES);
    }

    /**
     * Returns a {@link ConnectionPool} populated with handlers that
     * {@link Concourse#copyExistingConnection(Concourse) copy} the connection
     * information of the provided {@code concourse} instance. The pool has no
     * limit on the number of connections it can manage, but will attempt to use
     * previously created connections before establishing new ones on request.
     *
     * <p>
     * <strong>NOTE:</strong> The provided {@code concourse} connection will not
     * be a member of the returned {@link ConnectionPool} and its status will
     * not affect the status of any connections managed by the pool.
     * </p>
     *
     * @param concourse the {@link Concourse} connection to copy when populating
     *            the {@link ConnectionPool}
     * @return the populated {@link ConnectionPool}
     */
    public static ConnectionPool newCachedConnectionPool(Concourse concourse) {
        Supplier<Concourse> supplier = () -> concourse.copyConnection();
        return new CachedConnectionPool(supplier, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance described in the
     * {@code configFiles} on behalf of the user identified in the
     * {@code configFiles}, but will try to use previously created connections
     * before establishing new ones for any request.
     * 
     * @param config
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(Path... configFiles) {
        ConcourseClientConfiguration config = ConcourseClientConfiguration
                .from(configFiles);
        Supplier<Concourse> supplier = getConcourseSupplier(config.getHost(),
                config.getPort(), config.getUsername(),
                new String(config.getPassword()), config.getEnvironment());
        return new CachedConnectionPool(supplier, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance described in the
     * {@code config} on behalf of the user identified in the {@code config},
     * but will try to use previously created connections before establishing
     * new ones for any request.
     * 
     * @param config
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String config) {
        return newCachedConnectionPool(Paths.get(config));
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance defined in the client
     * {@code prefs} on behalf of the user defined in the client {@code prefs},
     * but will try to use previously created connections before establishing
     * new ones for any request.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String host, int port,
            String username, String password) {
        Supplier<Concourse> supplier = getConcourseSupplier(host, port,
                username, password, "");
        return new CachedConnectionPool(supplier, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance defined in the client
     * {@code prefs} on behalf of the user defined in the client {@code prefs},
     * but will try to use previously created connections before establishing
     * new ones for any request.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String host, int port,
            String username, String password, String environment) {
        Supplier<Concourse> supplier = getConcourseSupplier(host, port,
                username, password, environment);
        return new CachedConnectionPool(supplier, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides connections to the
     * Concourse instance defined in the client {@code prefs} on behalf of the
     * user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int)}.
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String prefs) {
        return newFixedConnectionPool(prefs, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides {@code poolSize}
     * connections to the Concourse instance defined in the client {@code prefs}
     * on behalf of the user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @param poolSize
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int)}.
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String prefs, int poolSize) {
        return newFixedConnectionPool(prefs, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} that provides connections to the
     * Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int, String, String, int)}
     *             .
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password) {
        return newFixedConnectionPool(host, port, username, password,
                DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides {@code poolSize}
     * connections to the Concourse instance at {@code host}:{@code port} on
     * behalf of the user identified by {@code username} and {@code password}.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int, String, String, int)}
     *             .
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password, int poolSize) {
        return newFixedConnectionPool(host, port, username, password, poolSize);
    }

    /**
     * Returns a {@link ConnectionPool} populated with handlers that
     * {@link Concourse#copyExistingConnection(Concourse) copy} the connection
     * information of the provided {@code concourse} instance. The pool will
     * contain {@code poolSize} connections. If all connections are active,
     * subsequent requests will block until a connection is returned.
     *
     * <p>
     * <strong>NOTE:</strong> The provided {@code concourse} connection will not
     * be a member of the returned {@link ConnectionPool} and its status will
     * not affect the status of any connections managed by the pool.
     * </p>
     *
     * @param concourse the {@link Concourse} connection to copy when populating
     *            the {@link ConnectionPool}
     * @param poolSize the number of connections in the pool
     * @return the populated {@link ConnectionPool}
     */
    public static ConnectionPool newFixedConnectionPool(Concourse concourse,
            int poolSize) {
        Supplier<Concourse> supplier = () -> concourse.copyConnection();
        return new FixedConnectionPool(supplier, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance defined in the {@code concourse_client.prefs} file
     * located in the working directory or using the default connection info if
     * no such file exists.
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(int poolSize) {
        return newFixedConnectionPool(poolSize, DEFAULT_CONFIG_FILES);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of
     * connections to the Concourse instance defined in the {@code configFiles}
     * on behalf of the user defined in the {@code configFiles}.
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param poolSize
     * @param config
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(int poolSize,
            Path... configFiles) {
        ConcourseClientConfiguration config = ConcourseClientConfiguration
                .from(configFiles);
        Supplier<Concourse> supplier = getConcourseSupplier(config.getHost(),
                config.getPort(), config.getUsername(),
                new String(config.getPassword()), config.getEnvironment());
        return new FixedConnectionPool(supplier, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of
     * connections to the Concourse instance defined in the {@code configFiles}
     * on behalf of the user defined in the {@code configFiles}.
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param config
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String config,
            int poolSize) {
        return newFixedConnectionPool(poolSize, Paths.get(config));
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String host, int port,
            String username, String password, int poolSize) {
        Supplier<Concourse> supplier = getConcourseSupplier(host, port,
                username, password, "");
        return new FixedConnectionPool(supplier, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String host, int port,
            String username, String password, String environment,
            int poolSize) {
        Supplier<Concourse> supplier = getConcourseSupplier(host, port,
                username, password, environment);
        return new FixedConnectionPool(supplier, poolSize);
    }

    /**
     * Return a {@link Supplier} that generates a new {@link Concourse}
     * connection from the provided credentials and connection information.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return the {@link Supplier}
     */
    private static Supplier<Concourse> getConcourseSupplier(String host,
            int port, String username, String password, String environment) {
        return () -> Concourse.connect(host, port, username, password,
                environment);
    }

    /**
     * The default connection pool size.
     */
    protected static final int DEFAULT_POOL_SIZE = 10;

    /**
     * The default configuration files to use if none are specified.
     */
    private static final Path[] DEFAULT_CONFIG_FILES = new Path[] {
            Paths.get("concourse_client.prefs"),
            Paths.get("concourse_client.yaml") };

    /**
     * A FIFO queue of connections that are available to be leased.
     */
    protected final Queue<Concourse> available;

    /**
     * The connections that have been leased from this pool.
     */
    private final Set<Concourse> leased;

    /**
     * A flag to indicate if the pool is currently open and operational.
     */
    private AtomicBoolean open = new AtomicBoolean(true);

    /**
     * The {@link Supplier} of {@link Concourse} connections.
     */
    protected final Supplier<Concourse> supplier;

    /**
     * Construct a new instance that provides {@link Concourse} connections that
     * copy the connection information from the provided {@code concourse}
     * handler.
     * <p>
     * <strong>NOTE:</strong>This constructor is provided for subclasses to
     * conveniently implement connection copying while abstracting away the
     * details of how to construct an appropriate {@link Supplier}.
     * </p>
     * 
     * @param concourse
     * @param poolSize
     */
    protected ConnectionPool(Concourse concourse, int poolSize) {
        this(() -> concourse.copyConnection(), poolSize);
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
    @Deprecated
    protected ConnectionPool(String host, int port, String username,
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
    @Deprecated
    protected ConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        this(() -> Concourse.connect(host, port, username, password,
                environment), poolSize);
    }

    /**
     * Construct a new instance.
     * 
     * @param supplier
     * @param poolSize
     */
    protected ConnectionPool(Supplier<Concourse> supplier, int poolSize) {
        this.supplier = supplier;
        this.available = buildQueue(poolSize);
        this.leased = Sets.newConcurrentHashSet();
        for (int i = 0; i < poolSize; ++i) {
            available.offer(supplier.get());
        }
        // Ensure that the client connections are forced closed when the JVM is
        // shutdown in case the user does not properly close the pool
        Runtime.getRuntime().addShutdownHook(new Thread(() -> forceClose()));
    }

    @Override
    public void close() throws Exception {
        Preconditions.checkState(isClosable(),
                "Cannot shutdown the connection pool "
                        + "until all the connections have been returned");
        if(open.compareAndSet(true, false)) {
            exitAllConnections();
        }
        else {
            throw new IllegalStateException("Connection pool is closed");
        }
    }

    /**
     * Return {@code true} if the pool has any available connections.
     * 
     * @return {@code true} if there are one or more available connections
     */
    public boolean hasAvailableConnection() {
        verifyOpenState();
        return available.peek() != null;
    }

    /**
     * Return {@code true} if this {@link ConnectionPool} has been closed.
     * 
     * @return a boolean that indicates whether the connection pool is closed or
     *         not
     */
    public boolean isClosed() {
        return !open.get();
    }

    /**
     * Return a previously requested connection back to the pool.
     * 
     * @param connection
     */
    public void release(Concourse connection) {
        verifyOpenState();
        verifyValidOrigin(connection);
        leased.remove(connection);
        if(connection.failed()) {
            // Dynamically detect when there is a client-specific failure and
            // replace it with a new one.
            connection.exit();
            connection = supplier.get();
        }
        available.offer(connection);
    }

    /**
     * Request a connection from the pool and block until one is available and
     * returned.
     * 
     * @return a connection
     */
    public Concourse request() {
        verifyOpenState();
        Concourse connection = getConnection();
        leased.add(connection);
        return connection;
    }

    /**
     * Return the {@link Queue} that will hold the connections.
     * 
     * @param size
     * 
     * @return the connections cache
     */
    protected abstract Queue<Concourse> buildQueue(int size);

    /**
     * Force the connection pool to close regardless of whether it is or is not
     * in a {@link #isClosable() closable} state.
     */
    protected void forceClose() {
        if(open.compareAndSet(true, false)) {
            exitConnections(available);
            exitConnections(leased);
        }
    }

    /**
     * Get a connection from the queue of {@code available} ones. The subclass
     * should use the correct method depending upon whether this method should
     * block or not.
     * 
     * @return the connection
     */
    protected abstract Concourse getConnection();

    /**
     * Exit all the connections managed of the pool that has a
     * {@link #available}.
     */
    private void exitAllConnections() {
        exitConnections(available);
    }

    /**
     * Close each of the given {@code connections} to {@link Concourse}
     * regardless of whether it is currently {@link #available} or
     * {@link #leased}.
     * 
     * @param connections an {@link Iterable} collection of connections.
     */
    private void exitConnections(Iterable<Concourse> connections) {
        for (Concourse concourse : connections) {
            boolean exited = false;
            while (!exited) {
                try {
                    concourse.exit();
                    exited = true;
                }
                catch (Exception e) {
                    // If a shutdown hook is used to close the connection pool,
                    // its possible to run into a situation where multiple
                    // threads operating on a client connection may trigger an
                    // out-of-sequence error with Thrift. If that is the case,
                    // keep retrying...
                    exited = false;
                }
            }

        }
    }

    /**
     * Return {@code true} if none of the connections are currently active.
     * 
     * @return {@code true} if the pool can be closed
     */
    private boolean isClosable() {
        return leased.isEmpty();
    }

    /**
     * Ensure that the connection pool is open. If it is not, throw an
     * IllegalStateException.
     */
    private void verifyOpenState() {
        Preconditions.checkState(open.get(), "Connection pool is closed");
    }

    /**
     * Verify that the {@code connection} was leased from this pool.
     * 
     * @param connection
     */
    private void verifyValidOrigin(Concourse connection) {
        if(!leased.contains(connection)) {
            throw new IllegalArgumentException(
                    "Cannot release the connection because it "
                            + "was not previously requested from this pool");
        }
    }
}
