/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade;

import static com.cinchapi.concourse.server.upgrade.UpgradeTask.getCurrentSystemVersion;

import java.util.Set;

import org.reflections.Reflections;

import com.cinchapi.concourse.server.upgrade.task.Upgrade2;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Reflection;
import com.google.common.collect.Sets;

/**
 * The {@link Initializer} is responsible for setting the schema version during
 * a new installation.
 * 
 * @author Jeff Nelson
 */
public class Initializer {

    static {
        Reflections.log = null; // turn off logging
    }

    /**
     * The package that contains all upgrade tasks.
     */
    private static final String[] pkgs = { "com.cinchapi.concourse.server.upgrade.task" };

    /**
     * Run all the upgrade tasks that are greater than the
     * {@link UpgradeTask#getCurrentSystemVersion() current system version}.
     */
    public static void runUpgrades() {
        int currentSystemVersion = initializeUpgradeFramework();

        // Find the new upgrade tasks
        Set<UpgradeTask> tasks = Sets.newTreeSet();
        for (String pkg : pkgs) {
            Reflections reflections = new Reflections(pkg);
            Set<Class<? extends UpgradeTask>> classes = reflections
                    .getSubTypesOf(UpgradeTask.class);
            classes.addAll(reflections.getSubTypesOf(SmartUpgradeTask.class));
            for (Class<? extends UpgradeTask> clazz : classes) {
                UpgradeTask task = Reflection.newInstance(clazz);
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
                            + "been force upgraded " + "to {}", task.version());
                }
                else {
                    System.exit(1); // fail fast because we assume
                                    // subsequent tasks depend on the one
                                    // that failed
                }
            }
        }
    }

    /**
     * Initialize the upgrade framework, if necessary.
     * <p>
     * Look at all the {@link UpgradeTask upgrade tasks} on the classpath
     * (without running any of them) and set the
     * {@link UpgradeTask#setCurrentSystemVersion(int) current system
     * version} to the largest seen.
     * </p>
     * 
     * @return the {@link UpgradeTask#getCurrentSystemVersion() current system
     *         version)}
     */
    private static int initializeUpgradeFramework() {
        int currentSystemVersion = getCurrentSystemVersion();
        if(currentSystemVersion == 0) { // it appears that no upgrade task
                                        // has ever run
            UpgradeTask theTask = null;
            // Go through the upgrade tasks and find the one with the largest
            // schema version.
            for (String pkg : pkgs) {
                Reflections reflections = new Reflections(pkg);
                Set<Class<? extends UpgradeTask>> classes = reflections
                        .getSubTypesOf(UpgradeTask.class);
                classes.addAll(reflections
                        .getSubTypesOf(SmartUpgradeTask.class));
                for (Class<? extends UpgradeTask> clazz : classes) {
                    UpgradeTask task = Reflection.newInstance(clazz);
                    if(theTask == null || task.version() > theTask.version()) {
                        theTask = task;
                    }
                }
            }
            UpgradeTask.setCurrentSystemVersion(theTask.version());
            Logger.info("The upgrade framework has been initialized "
                    + "with a system version of {}", theTask.version());
            return theTask.version();
        }
        return currentSystemVersion;
    }

}
