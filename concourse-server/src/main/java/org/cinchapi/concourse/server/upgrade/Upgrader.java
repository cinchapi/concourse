/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.server.upgrade;

import java.util.Set;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.upgrade.task.Upgrade2;
import org.cinchapi.concourse.util.Logger;
import org.reflections.Reflections;

import com.google.common.collect.Sets;

import static org.cinchapi.concourse.server.upgrade.UpgradeTask.getCurrentSystemVersion;

/**
 * The {@link Upgrader} is responsible for running the latest upgrade tasks on
 * the storage directories. This script is typically called when a user upgrades
 * ConcourseServer from one version to the next.
 * 
 * @author Jeff Nelson
 */
public class Upgrader {

    static {
        Reflections.log = null; // turn off logging
    }

    /**
     * The package that contains all upgrade tasks.
     */
    private static final String[] pkgs = { "org.cinchapi.concourse.server.upgrade.task" };

    /**
     * Run the program...
     * 
     * @param args
     * @throws ReflectiveOperationException
     */
    public static void main(String... args) throws ReflectiveOperationException {
        // Temporarily turn on console logging so the user can see the log
        // messages while the upgrades happen. Although we revert the setting
        // after the tasks have finished, console logging will be enabled for
        // the duration of this JVM's lifecycle. That should be okay as long as
        // the Upgrader runs in a different JVM that the normal ConcourseServer
        // process.
        boolean enableConsoleLogging = GlobalState.ENABLE_CONSOLE_LOGGING;
        GlobalState.ENABLE_CONSOLE_LOGGING = true;
        try {
            int currentSystemVersion = getCurrentSystemVersion();

            // Find the new upgrade tasks
            Set<UpgradeTask> tasks = Sets.newTreeSet();
            for (String pkg : pkgs) {
                Reflections reflections = new Reflections(pkg);
                Set<Class<? extends UpgradeTask>> classes = reflections
                        .getSubTypesOf(UpgradeTask.class);
                classes.addAll(reflections
                        .getSubTypesOf(SmartUpgradeTask.class));
                for (Class<? extends UpgradeTask> clazz : classes) {
                    UpgradeTask task = clazz.newInstance();
                    if(task.version() > currentSystemVersion) {
                        tasks.add(task);
                    }
                }
            }

            // Run the new upgrade tasks
            for (UpgradeTask task : tasks) {
                try {
                    task.run();
                }
                catch (Exception e) {
                    if(task instanceof Upgrade2) {
                        // CON-137: Even if Upgrade2 fails and we can't migrate
                        // data, still set the system version so we aren't
                        // blocked on this task in the future.
                        UpgradeTask.setCurrentSystemVersion(task.version());
                        Logger.info("Due to a bug in a previous "
                                + "release, the system version has "
                                + "been force upgraded " + "to {}",
                                task.version());
                    }
                    else {
                        System.exit(1); // fail fast because we assume
                                        // subsequent tasks depend on the one
                                        // that failed
                    }
                }
            }
            System.exit(0);

        }
        finally {
            GlobalState.ENABLE_CONSOLE_LOGGING = enableConsoleLogging;
        }
    }

}
