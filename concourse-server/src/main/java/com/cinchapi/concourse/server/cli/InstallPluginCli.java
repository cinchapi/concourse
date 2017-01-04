/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;

import com.cinchapi.concourse.cli.util.CommandLineInterfaces;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;

/**
 * A cli for install plugins.
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
        if(Files.exists(Paths.get(path))) {
            try {
                AtomicBoolean done = new AtomicBoolean(false);
                Thread tracker = new Thread(() -> {
                    double percent = 0;
                    Threads.sleep(1000);
                    while (!done.get()) {
                        System.out.print("\r" + CommandLineInterfaces
                                .renderPercentDone(percent));
                        percent = percent + ((100.0 - percent) / 32.0);
                        Threads.sleep(1000);
                    }
                });
                tracker.setDaemon(true);
                tracker.start();
                client.installPluginBundle(path, token);
                done.set(true);
                System.out.println(
                        "\r" + CommandLineInterfaces.renderPercentDone(100));
            }
            catch (TException e) {
                die(e.getMessage());
            }
            System.out.println("Successfully installed " + path);
        }
        else {
            throw new UnsupportedOperationException(
                    com.cinchapi.concourse.util.Strings
                            .format("Cannot download plugin bundle '{}'. Please "
                                    + "manually download the plugin and "
                                    + "provide its local path to the "
                                    + "installer", plugin));
        }
    }
}
