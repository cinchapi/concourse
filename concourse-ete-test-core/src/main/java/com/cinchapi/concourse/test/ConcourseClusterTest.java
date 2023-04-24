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
package com.cinchapi.concourse.test;

import java.nio.file.Path;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.logging.Logger;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.automation.cluster.ManagedConcourseCluster;
import com.cinchapi.concourse.automation.developer.ConcourseCodebase;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A {@link ConcourseClusterTest} is one that interacts with a Concourse Cluster
 * deployment via the public Client API. This base class handles boilerplate
 * logic for creating a new cluster for each test and managing resources.
 * <p>
 * <ul>
 * <li>Specify the server version for each node using the {@link #nodeVersion()}
 * method.</li>
 * <li>Specify actions to take before each test using the
 * {@link #beforeEachTest()} method.</li>
 * <li>Specify actions to take after each test using the
 * {@link #afterEachTest()} method.</li>
 * <li>Specify actions to take before each test cluster starts using the
 * {@link #beforeEachClusterStart()} method.</li>
 * </ul>
 * </p>
 * <p>
 * Additionally, be sure to provide implementations for other abstract methods
 * (e.g., {@link #clusterSize()}, {@link #replicationFactor()}, etc) to define
 * the desired specification of the cluster.
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class ConcourseClusterTest {

    // Initialization for all tests
    static {
        System.setProperty("test", "true");
    }

    /**
     * Return the number of nodes that should makeup the cluster.
     * 
     * @return the number of nodes
     */
    public abstract int clusterSize();

    /**
     * Return the replication factor for the cluster.
     * 
     * @return the replication factor
     */
    public abstract int replicationFactor();

    /**
     * Return the version to use for each node in the cluster.
     * 
     * @return the node version
     */
    public abstract String nodeVersion();

    /**
     * The client allows the subclass to define tests that perform actions
     * against the test {@link #server} using the public API.
     */
    protected Concourse client = null;

    /**
     * A new cluster is created for every test. The subclass can perform
     * lifecycle management operations on the cluster using this variable and
     * may also interact via the {@link #client} API.
     */
    protected ManagedConcourseCluster cluster = null;

    @Rule
    public final TestWatcher __watcher = new TestWatcher() {

        @Override
        protected void succeeded(Description description) {
            cluster.destory();
        }

        @Override
        protected void failed(Throwable t, Description description) {
            System.err.println("TEST FAILURE in " + description.getMethodName()
                    + ": " + t.getMessage());
            System.err.println("---");
            System.err.println(Variables.dump());
            System.err.println(AnyStrings.format(
                    "NOTE: The test failed, so the server installation for each node has NOT been deleted. Please manually delete the directories after inspecting its content:"));
            cluster.nodes().forEach(node -> {
                System.out.println(node.directory());
                node.setDestroyOnExit(false);
            });
            cluster.stop();
        }

        @Override
        protected void finished(Description description) {
            afterEachTest();
            client.exit();
            client = null;
            cluster = null;
        }

        /**
         * A {@link Logger} to print information about the test case.
         */
        protected Logger log = Logger.console(this.getClass().getName());

        @Override
        protected void starting(Description description) {
            Variables.clear();
            int clusterSize = clusterSize();
            Preconditions.checkState(clusterSize >= 1,
                    "clusterSize() must return a value that is >= 1");
            int port = Networking.getOpenPort();
            --clusterSize;
            Integer[] ports;
            if(clusterSize > 0) {
                ArrayBuilder<Integer> ab = ArrayBuilder.builder();
                for (int i = 0; i < clusterSize; ++i) {
                    ab.add(Networking.getOpenPort());
                }
                ports = ab.build();
            }
            else {
                ports = Array.containing();
            }
            String version = nodeVersion();
            if(version.equalsIgnoreCase(
                    ClientServerTest.LATEST_SNAPSHOT_VERSION)) {
                ConcourseCodebase codebase = ConcourseCodebase.get();
                try {
                    log.info(
                            "Creating an installer for the latest "
                                    + "version using the code in {}",
                            codebase.path());
                    Path installer = codebase.installer();
                    if(!Strings.isNullOrEmpty(installer.toString())) {
                        cluster = ManagedConcourseCluster.create(installer,
                                port, ports);
                    }
                    else {
                        throw new RuntimeException(
                                "An unknown error occurred when trying to build the installer");
                    }
                }
                catch (Exception e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }
            else {
                cluster = ManagedConcourseCluster.create(version, port, ports);
            }
            // Update the cluster specification based on the desired test
            // parameters
            cluster.spec().replicationFactor(replicationFactor());

            // Start the cluster
            beforeEachClusterStart();
            cluster.start();
            client = cluster.connect();
            beforeEachTest();
        }

    };

    /**
     * This method is provided for the subclass to specify additional behaviour
     * to be run after each test is done. The subclass should define such logic
     * in this method as opposed to a test watcher.
     */
    protected void afterEachTest() {}

    /**
     * This method is provided for the subclass to specify additional behaviour
     * to be run before each test begins. The subclass should define such logic
     * in this method as opposed to a test watcher.
     */
    protected void beforeEachTest() {}

    /**
     * This method is provided for the subclass to specify additional behaviour
     * to run before each test's cluster is started and prior to the
     * {@link #beforeEachTest()} method.
     */
    protected void beforeEachClusterStart() {}
}
