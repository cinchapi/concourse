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
package com.cinchapi.concourse.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.process.Processes;
import com.cinchapi.common.process.Processes.ProcessResult;
import com.cinchapi.concourse.automation.developer.ConcourseCodebase;
import com.google.common.io.Files;

/**
 * A {@link ClientServerTest} that {@link #preUpgradeActions() creates state} in
 * an {@link #getInitialServerVersion() initial version} and then upgrades the
 * server to the latest version. Each unit test in the class is then run against
 * the upgraded server. The purpose of these kinds of test is to ensure that an
 * upgrade does not ruin any data, etc.
 * 
 * @author Jeff Nelson
 */
public abstract class UpgradeTest extends ClientServerTest {

    @Override
    protected final String getServerVersion() {
        return getInitialServerVersion();
    }

    @Override
    protected final void beforeEachTest() {
        super.beforeEachTest();
        try {
            log.info("Running pre upgrade actions...");
            preUpgradeActions();
            server.stop();
            ConcourseCodebase codebase = ConcourseCodebase.get();
            Path installer = codebase.installer();
            File src = installer.toFile();
            File dest = server.directory().resolve("concourse-server.bin")
                    .toFile();
            Files.copy(src, dest);
            // Run the upgrade from the installer
            log.info("Upgrading Concourse Server...");
            Process proc = new ProcessBuilder("sh", dest.getAbsolutePath(),
                    "--", "skip-integration")
                            .directory(server.directory().toFile()).start();
            ProcessResult result = Processes.waitForSuccessfulCompletion(proc);
            for (String line : result.out()) {
                log.info(line);
            }
            server.start();
            client = server.connect();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Get the initial server version from which to upgrade.
     * 
     * @return the initial version
     */
    protected abstract String getInitialServerVersion();

    /**
     * Alias for {@link #beforeEachTest()}. These are the actions to run before
     * each test prior to upgrading to the latest server version.
     */
    protected abstract void preUpgradeActions();

}
