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
package com.cinchapi.concourse.test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.config.ConcourseServerPreferences;
import com.cinchapi.concourse.plugin.build.PluginBundleGenerator;
import com.cinchapi.concourse.server.ManagedConcourseServer;
import com.cinchapi.concourse.util.ConcourseCodebase;
import com.cinchapi.concourse.util.ConcourseServerDownloader;
import com.google.common.base.Strings;

/**
 * A {@link ClientServerTest} is one that interacts with a Concourse server via
 * the public client API. This base class handles boilerplate logic for creating
 * a new server for each test and managing resources.
 * <ul>
 * <li>Specify the server version or custom installer file to test against using
 * the {@link #getServerVersion()} method.</li>
 * <li>Specify actions to take before each test using the
 * {@link #beforeEachTest()} method.</li>
 * <li>Specify actions to take after each test using the
 * {@link #afterEachTest()} method.</li>
 * </ul>
 * 
 * @author jnelson
 */
public abstract class ClientServerTest {

    // Initialization for all tests
    static {
        System.setProperty("test", "true");
    }

    /**
     * A constant that indicates the latest version should be used in
     * {@link #getServerVersion()}.
     */
    public final static String LATEST_SNAPSHOT_VERSION = "latest";

    /**
     * The default initialization hook for installed
     * {@link ManagedConcourseServer servers}.
     */
    private static final Consumer<ConcourseServerPreferences> SERVER_NO_OP_INITIALIZER = config -> {};

    /**
     * The client allows the subclass to define tests that perform actions
     * against the test {@link #server} using the public API.
     */
    protected Concourse client = null;

    /**
     * A new server is created for every test. The subclass can perform
     * lifecycle and management operations on the server using this variable and
     * may also interact via the {@link #client} API.
     */
    protected ManagedConcourseServer server = null;

    /**
     * A cache of the {@link Path} to the provided, downloaded or generated
     * installer file. This is determined based on what is returned from
     * {@link #getServerVersion()}.
     */
    private Path installerPath = null;

    /**
     * This watcher clears previously registered {@link Variables} on startup
     * and dumps them in the event of failure.
     */
    @Rule
    public final TestWatcher __watcher = new TestWatcher() {

        @Override
        protected void failed(Throwable t, Description description) {
            System.err.println("TEST FAILURE in " + description.getMethodName()
                    + ": " + t.getMessage());
            System.err.println("---");
            System.err.println(Variables.dump());
            if(PluginTest.class
                    .isAssignableFrom(ClientServerTest.this.getClass())) {

            }
            System.err.println(AnyStrings.format(
                    "NOTE: The test failed, so the server installation at {} has "
                            + "NOT been deleted. Please manually delete the directory after "
                            + "inspecting its content",
                    server.getInstallDirectory()));
            server.destroyOnExit(false);
            server.stop();
        }

        @Override
        protected void finished(Description description) {
            afterEachTest();
            client.exit();
            client = null;
            server = null;
        }

        @Override
        protected void starting(Description description) {
            Variables.clear();
            if(installerPath == null) {
                String serverVersion = getServerVersion();
                Path path = Paths.get(serverVersion);
                if(Files.exists(path)) {
                    // if #getServerVersion returns a valid path, then assume
                    // its an installer and pass it along for construction
                    installerPath = path;
                }
                else if(serverVersion
                        .equalsIgnoreCase(LATEST_SNAPSHOT_VERSION)) {
                    ConcourseCodebase codebase = ConcourseCodebase
                            .cloneFromGithub();
                    try {
                        log.info(
                                "Creating an installer for the latest version using the code in {}",
                                codebase.getPath());
                        installerPath = Paths.get(codebase.buildInstaller());
                        if(Strings.isNullOrEmpty(installerPath.toString())) {
                            throw new RuntimeException(
                                    "An unknown error occurred when trying to build the installer");
                        }
                    }
                    catch (Exception e) {
                        throw CheckedExceptions.wrapAsRuntimeException(e);
                    }
                }
                else if(installerPath() == null) {
                    installerPath = Paths.get(
                            ConcourseServerDownloader.download(serverVersion));
                }
                else {
                    installerPath = installerPath().toPath();
                }
            }
            server = installServer(SERVER_NO_OP_INITIALIZER);
            client = server.connect();
            beforeEachTest();
        }

        @Override
        protected void succeeded(Description description) {
            server.destroy();
        }

    };

    /**
     * A {@link Logger} to print information about the test case.
     */
    protected Logger log = LoggerFactory.getLogger(getClass());

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
     * This method is provided for the subclass to specify the appropriate
     * release version number OR path to an installer file (i.e. an unreleased
     * SNAPSHOT version) to test against.
     * 
     * @return the version number
     */
    protected abstract String getServerVersion();

    /**
     * This method is provided for the subclass to specify the path to a custom
     * installer (i.e. testing against a SNAPSHOT version of the server). If
     * this method returns a null or empty string, the default installer is
     * used.
     * 
     * @return the custom installer path
     * @deprecated Return the path to the installer in
     *             {@link #getServerVersion()} instead
     */
    @Nullable
    @Deprecated
    protected File installerPath() {
        return null;
    }

    /**
     * Reinstall the {@link ManagedConcourseServer} and update the
     * {@link #server} to reference reflect the new instance.
     * <p>
     * The existing server is stopped and destroyed before a new server is
     * installed and started.
     * </p>
     * <p>
     * <strong>NOTE:</strong> Reinstalling the server will reset any changes to
     * configuration and data.
     * </p>
     */
    protected final ManagedConcourseServer reinstallServer() {
        return reinstallServer(SERVER_NO_OP_INITIALIZER);
    }

    /**
     * Reinstall the {@link ManagedConcourseServer} and update the
     * {@link #server} to reference reflect the new instance.
     * <p>
     * The existing server is stopped and destroyed before a new server is
     * installed and started.
     * </p>
     * <p>
     * <strong>NOTE:</strong> Reinstalling the server will reset any changes to
     * configuration and data.
     * </p>
     * 
     * @param initializer a {@link Consumer} that is run on the newly
     *            installed server's configuration <strong>BEFORE</strong> it is
     *            started; useful for setting initial state
     */
    protected final ManagedConcourseServer reinstallServer(
            Consumer<ConcourseServerPreferences> initializer) {
        server.stop();
        server.destroy();
        server = installServer(initializer);
        return server;
    }

    /**
     * Install a new {@link ManagedConcourseServer} (as well as any plugins if
     * this is a {@link PluginTest}). Afterwards, run the {@code initializer}
     * and {@link ManagedConcourseServer#start() start} the server.
     * <p>
     * Successful installation requires that the {@link #installerPath} has been
     * set and the {@link #server} is null.
     * </p>
     * <p>
     * The return {@link ManagedConcourseServer server} will be
     * {@link ManagedConcourseServer#start() started}, so if actions are
     * required prior to starting, define them in the {@code #initializer}.
     * </p>
     * 
     * @param initializer a {@link Consumer} that is run on the newly
     *            installed server's configuration <strong>BEFORE</strong> it is
     *            started; useful for setting initial state
     */
    private ManagedConcourseServer installServer(
            Consumer<ConcourseServerPreferences> initializer) {
        Verify.that(installerPath != null, "Invalid installer path");
        ManagedConcourseServer server = ManagedConcourseServer
                .manageNewServer(installerPath.toFile());
        Path pluginBundlePath = null;
        if(PluginTest.class
                .isAssignableFrom(ClientServerTest.this.getClass())) {
            // Turn the current codebase into a plugin bundle and place it
            // inside the install directory
            log.info("Generating plugin to install in Concourse Server");
            pluginBundlePath = PluginBundleGenerator.generateBundleZip();
        }
        initializer.accept(server.prefs());
        server.start();
        if(pluginBundlePath != null) {
            server.installPlugin(pluginBundlePath);
        }
        return server;
    }

}
