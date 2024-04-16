/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.automation.cluster;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.logging.Logger;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.automation.server.ManagedConcourseServer;
import com.cinchapi.concourse.config.ConcourseClusterSpecification;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.Preconditions;

/**
 * A controller for a group of {@link ManagedConcourseServer managed} Concourse
 * Server instances that form a distributed cluster.
 * <p>
 * Use one of the "create" factory methods to instantiate a
 * {@link ManagedConcourseCluster}.
 * </p>
 * <p>
 * Before {@link #start() starting} a {@link ManagedConcourseCluster}, the
 * specification (e.g., replication factor, etc) can be tweaked by accessing
 * the {@link #spec()}.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class ManagedConcourseCluster {

    /**
     * Return a {@link ManagedConcourseCluster} with nodes that are listening on
     * the defined {@code ports}.
     * <p>
     * Each node is housed under a random directory and the {@code installer} is
     * used to install a new Concourse Server instance on each node that does
     * not already have one.
     * </p>
     * <p>
     * Before {@link #start() starting} a {@link ManagedConcourseCluster}, the
     * specification (e.g., replication factor, etc) can be tweaked by accessing
     * the {@link #spec()}.
     * </p>
     * 
     * @param installer
     * @param port
     * @param ports
     * @return the {@link ManagedConcourseCluster}
     */
    public static ManagedConcourseCluster create(Path installer, int port,
            Integer... ports) {
        return create(getNewInstallationDirectory(), installer, port, ports);
    }

    /**
     * Return a {@link ManagedConcourseCluster} with nodes that are listening on
     * the defined {@code ports}.
     * <p>
     * Each node is housed under {@code directory} and the {@code installer} is
     * used to install a new Concourse Server instance on each node that does
     * not already have one.
     * </p>
     * <p>
     * Before {@link #start() starting} a {@link ManagedConcourseCluster}, the
     * specification (e.g., replication factor, etc) can be tweaked by accessing
     * the {@link #spec()}.
     * </p>
     * 
     * @param installer
     * @param port
     * @param ports
     * @return the {@link ManagedConcourseCluster}
     */
    public static ManagedConcourseCluster create(Path directory, Path installer,
            int port, Integer... ports) {
        ports = ArrayBuilder.<Integer> builder().add(port).add(ports).build();
        List<ManagedConcourseServer> nodes = new ArrayList<>();
        for (int p : ports) {
            Path path = directory.resolve(Integer.toString(p));
            ManagedConcourseServer node;
            if(path.toFile().exists()) {
                node = ManagedConcourseServer.open(path);
            }
            else {
                node = ManagedConcourseServer.install(installer, path);
            }
            nodes.add(node);
        }
        return new ManagedConcourseCluster(nodes);
    }

    /**
     * Return a {@link ManagedConcourseCluster} with nodes that are listening on
     * the defined {@code ports}.
     * <p>
     * Each node is housed under {@code directory} and a new Concourse Server
     * instance at {@code version} is installed on each node that does
     * not already have one.
     * </p>
     * <p>
     * NOTE: If a node already has a Concourse Server
     * instance, that instance is not upgraded or downgraded to be consistent
     * with {@code version}
     * </p>
     * <p>
     * Before {@link #start() starting} a {@link ManagedConcourseCluster}, the
     * specification (e.g., replication factor, etc) can be tweaked by accessing
     * the {@link #spec()}.
     * </p>
     * 
     * @param installer
     * @param port
     * @param ports
     * @return the {@link ManagedConcourseCluster}
     */
    public static ManagedConcourseCluster create(Path directory, String version,
            int port, Integer... ports) {
        ports = ArrayBuilder.<Integer> builder().add(port).add(ports).build();
        List<ManagedConcourseServer> nodes = new ArrayList<>();
        for (int p : ports) {
            Path path = directory.resolve(Integer.toString(p));
            ManagedConcourseServer node;
            if(path.toFile().exists()) {
                node = ManagedConcourseServer.open(path);
            }
            else {
                node = ManagedConcourseServer.install(version, path);
            }
            nodes.add(node);
        }
        return new ManagedConcourseCluster(nodes);
    }

    /**
     * Return a {@link ManagedConcourseCluster} with nodes that are listening on
     * the defined {@code ports}.
     * <p>
     * Each node is housed under a random directory and a new Concourse Server
     * instance at {@code version} is installed on each node that does
     * not already have one.
     * </p>
     * <p>
     * NOTE: If a node already has a Concourse Server
     * instance, that instance is not upgraded or downgraded to be consistent
     * with {@code version}
     * </p>
     * <p>
     * Before {@link #start() starting} a {@link ManagedConcourseCluster}, the
     * specification (e.g., replication factor, etc) can be tweaked by accessing
     * the {@link #spec()}.
     * </p>
     * 
     * @param installer
     * @param port
     * @param ports
     * @return the {@link ManagedConcourseCluster}
     */
    public static ManagedConcourseCluster create(String version, int port,
            Integer... ports) {
        return create(getNewInstallationDirectory(), version, port, ports);
    }

    /**
     * Return a random directory.
     * 
     * @return the {@link Path} to the directory
     */
    private static Path getNewInstallationDirectory() {
        return Paths.get(FileOps.tempDir("concourse-cluster"));
    }

    /**
     * The nodes in the cluster.
     */
    private final List<ManagedConcourseServer> nodes;

    /**
     * The cluster specification.
     */
    private final ConcourseClusterSpecification spec;

    /**
     * Random.
     */
    private final Random random = new Random();

    /**
     * A {@link Logger} for this class.
     */
    private final Logger log = Logger.console(this.getClass().getName());

    /**
     * Construct a new instance.
     * 
     * @param nodes
     */
    private ManagedConcourseCluster(List<ManagedConcourseServer> nodes) {
        Preconditions.checkArgument(!nodes.isEmpty());
        this.spec = ConcourseClusterSpecification.defaults();
        this.nodes = nodes;
    }

    /**
     * Return a client connection to a random {@link ManagedConcourseServer
     * node} in the cluster.
     * 
     * @return the Client
     */
    public Concourse connect() {
        return node().connect();
    }

    /**
     * Return a client connection to a random {@link ManagedConcourseServer
     * node} in the cluster using the {@code username} and {@code password}.
     * 
     * @param username
     * @param password
     * @return the Client
     */
    public Concourse connect(String username, String password) {
        return node().connect(username, password);
    }

    /**
     * Return a client connection to {@code environment} of a random
     * {@link ManagedConcourseServer node} in the cluster using the
     * {@code username} and {@code password}.
     * 
     * @param username
     * @param password
     * @param environment
     * @return the Client
     */
    public Concourse connect(String username, String password,
            String environment) {
        return node().connect(username, password, environment);
    }

    /**
     * {@link ManagedConcourseServer#destroy() Destroy} all the nodes in the
     * cluster.
     */
    public void destory() {
        for (ManagedConcourseServer node : nodes) {
            node.destroy(); // TODO: do this async?
        }
    }

    /**
     * Return the {@link ManagedConcourseServer node} that is listening on port
     * {@code port}.
     * 
     * @param port
     * @return the node
     */
    public ManagedConcourseServer node(int port) {
        for (ManagedConcourseServer node : nodes) {
            if(node.getClientPort() == port) {
                return node;
            }
        }
        throw new IllegalArgumentException(
                "No node in the cluster is bound to port " + port);
    }

    /**
     * Return all of the {@link ManagedConcourseServer nodes} in this
     * {@link ManagedConcourseCluster cluster}.
     * 
     * @return the nodes
     */
    public Iterable<ManagedConcourseServer> nodes() {
        return Collections.unmodifiableList(nodes);
    }

    /**
     * {@link ManagedConcourseServer#start() Start} all the nodes in the
     * cluster.
     */
    public void start() {
        sync();
        // TODO: do this async?
        for (ManagedConcourseServer node : nodes) {
            // NOTE: Each node is setup for remote debugging to diagnose unit
            // test failures.
            int remoteDebuggerPort = Networking.getOpenPort();
            node.config().set("remote_debugger_port", remoteDebuggerPort);
            node.start();
            log.info(
                    "The node on localhost:{} is setup for remote debugging on port {}",
                    node.config().getClientPort(), remoteDebuggerPort);
        }
    }

    /**
     * {@link ManagedConcourseServer#stop() Stop} all the nodes in the
     * cluster.
     */
    public void stop() {
        for (ManagedConcourseServer node : nodes) {
            node.stop(); // TODO: do this async?
        }
    }

    /**
     * Return the {@link ConcourseClusterSpecification}.
     * 
     * @return the spec
     */
    public ConcourseClusterSpecification spec() {
        return spec;
    }

    /**
     * Return a random {@link ManagedConcourseServer node}.
     * 
     * @return a random node
     */
    private ManagedConcourseServer node() {
        return nodes.get(random.nextInt(nodes.size()));
    }

    /**
     * Sync the cluster specification across all the {@link #nodes} in the
     * cluster.
     */
    private void sync() {
        spec.nodes(
                nodes.stream().map(node -> "localhost:" + node.getClientPort())
                        .collect(Collectors.toList()));
        for (ManagedConcourseServer node : nodes) {
            spec.publish(node.config());
        }
    }

}
