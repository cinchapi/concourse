/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.ete.bugrepro;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.server.ManagedConcourseServer;
import com.cinchapi.concourse.util.ConcourseCodebase;
import com.cinchapi.concourse.util.Processes;
import com.google.common.io.Files;

/**
 * Unit test to reproduce issue described in GH-426
 *
 * @author Jeff Nelson
 */
public class GH426 {

    @Test
    public void testReproGH426() {
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        String installer = codebase.buildInstaller();
        ManagedConcourseServer server = ManagedConcourseServer
                .manageNewServer(new File(installer));
        server.start();
        Concourse client = server.connect();
        long record = client.add("name", "jeff");
        Assert.assertEquals("jeff", client.get("name", record));
    }

    @Test
    public void testFreshInstallExistingData() throws IOException {
        // Ensure that commit 237556de81031c919fc8add3b773618ef750ca48 still
        // works as expected
        ManagedConcourseServer server = ManagedConcourseServer
                .manageNewServer("0.10.5");
        server.start();
        Concourse client = server.connect();
        long record = client.add("name", "jeff");
        server.stop();
        String directory = server.getInstallDirectory();
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        String installer = codebase.buildInstaller();
        File src = new File(installer);
        File dest = new File(
                server.getInstallDirectory() + "/concourse-server.bin");
        Files.copy(src, dest);
        // Run the upgrade from the installer
        System.out.println("Upgrading Concourse Server...");
        Process proc = new ProcessBuilder("sh", dest.getAbsolutePath(), "--",
                "skip-integration")
                        .directory(new File(server.getInstallDirectory()))
                        .start();

        Processes.waitForSuccessfulCompletion(proc);
        for (String line : Processes.getStdOut(proc)) {
            System.out.println(line);
        }
        server = ManagedConcourseServer.manageExistingServer(directory);
        server.start();
        client = server.connect();
        Assert.assertEquals("jeff", client.get("name", record));
    }

}
