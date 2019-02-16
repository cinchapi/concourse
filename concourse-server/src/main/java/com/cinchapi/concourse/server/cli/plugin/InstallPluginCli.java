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
package com.cinchapi.concourse.server.cli.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.cli.util.CommandLineInterfaces;
import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.thrift.ManagementException;
import com.cinchapi.concourse.util.FileOps;

/**
 * A cli for installing plugins.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Install a local or marketplace plugin")
class InstallPluginCli extends PluginCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public InstallPluginCli(String[] args) {
        super(new PluginCli.PluginOptions(), args);
    }

    @Override
    protected void doTask(Client client) {
        String plugin = options.args.get(0);
        String path = FileSystem.expandPath(plugin, getLaunchDirectory());
        Path path0 = Paths.get(path);
        if(Files.isDirectory(path0)) {
            System.out.println(AnyStrings.format(
                    "Attempting to install plugin bundles from '{}'", path0));
            AtomicInteger installed = new AtomicInteger(0);
            FileOps.newDirectoryStream(path0).forEach((file) -> {
                if(!Files.isDirectory(file)) {
                    try {
                        installPluginBundle(client, file);
                        installed.incrementAndGet();
                    }
                    catch (TException e) { // Log the error as a warning and
                                           // keep iterating through the
                                           // directory
                        System.err.println("[WARN] " + e.getMessage());
                    }
                }
                else {
                    System.err.println(AnyStrings.format(
                            "[WARN] Skipping '{}' because it is a directory",
                            file));
                }
            });
            System.out.println(
                    AnyStrings.format("Installed a total of {} plugin{}",
                            installed.get(), installed.get() == 1 ? "" : "s"));
        }
        else if(Files.exists(path0)) {
            try {
                installPluginBundle(client, path0);
            }
            catch (TException e) {
                die(e.getMessage());
            }
        }
        else {
            throw new UnsupportedOperationException(AnyStrings.format(
                    "Cannot download plugin bundle '{}'. Please "
                            + "manually download the plugin and "
                            + "provide its local path to the " + "installer",
                    plugin));
        }
    }

    @Override
    protected boolean requireArgs() {
        return true;
    }

    /**
     * Use the {@code client} to install the plugin bundle at {@code path}.
     * 
     * @param client a {@link Client} to use for server interaction
     * @param path the {@link Path} to the plugin bundle
     * @throws ManagementException
     * @throw TException
     */
    private void installPluginBundle(Client client, Path path)
            throws ManagementException, TException {
        AtomicBoolean done = new AtomicBoolean(false);
        Thread tracker = new Thread(() -> {
            double percent = 0;
            Threads.sleep(1000);
            while (!done.get()) {
                System.out.print("\r"
                        + CommandLineInterfaces.renderPercentDone(percent));
                percent = percent + ((100.0 - percent) / 32.0);
                Threads.sleep(1000);
            }
        });
        tracker.setDaemon(true);
        tracker.start();
        client.installPluginBundle(path.toString(), token);
        done.set(true);
        System.out.println("\r" + CommandLineInterfaces.renderPercentDone(100));
        System.out.println("Successfully installed " + path);
    }
}
