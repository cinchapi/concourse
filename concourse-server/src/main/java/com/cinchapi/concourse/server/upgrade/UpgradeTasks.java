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
package com.cinchapi.concourse.server.upgrade;

import static com.cinchapi.concourse.server.upgrade.UpgradeTask.getCurrentSystemVersion;

import java.util.Set;

import org.reflections.Reflections;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Logger;
import com.google.common.collect.Sets;

/**
 * A collection of methods that are responsible for bootstrapping and managing
 * the upgrade process.
 * 
 * @author Jeff Nelson
 */
public final class UpgradeTasks {

    static {
        Reflections.log = null; // turn off logging
    }

    /**
     * The package that contains all upgrade tasks.
     */
    private static final String[] pkgs = {
            "com.cinchapi.concourse.server.upgrade.task" };

    /**
     * Run all the upgrade tasks that are greater than the
     * {@link UpgradeTask#getCurrentSystemVersion() current system version}.
     */
    public static void runLatest() {
        int currentSystemVersion = 0;
        try {
            currentSystemVersion = bootstrap();
        }
        catch (Exception e) {
            String user = System.getProperty("user.name");
            Logger.upgradeError(
                    "An error occurred while trying to bootstrap the upgrade framework, "
                            + "which usually indicates that Concourse Server is configured to store "
                            + "data in one or more locations where the current user ({}) does not "
                            + "have write permission. Please check the prefs file at {} to make sure "
                            + "you have properly configured the buffer_directory and database_directory. "
                            + "If those properties are properly configured, please give \"{}\" write "
                            + "permission to those directories.",
                    user, GlobalState.getPrefsFilePath(), user);
            throw e;
        }

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
                    if(currentSystemVersion >= task.requiresVersion()) {
                        tasks.add(task);
                    }
                    else {
                        String msg = AnyStrings.format(
                                "Cannot upgrade because system version {} is required, "
                                        + "but the current system version is {}. Please upgrade "
                                        + "to an earlier version of Concourse before upgrading to "
                                        + "this version",
                                task.requiresVersion(), currentSystemVersion);
                        Logger.upgradeError(msg);
                        throw new IllegalStateException(msg);
                    }
                }
            }
        }

        // Run the new upgrade tasks
        Logger.upgradeInfo("Found {} upgrade task{}", tasks.size(),
                tasks.size() != 1 ? "s" : "");
        for (UpgradeTask task : tasks) {
            try {
                task.run();
            }
            catch (Exception e) {
                Logger.upgradeError("Must perform a rollback of {}", task);
                task.rollback();
                throw e; // fail fast because we assume subsequent tasks
                         // depend on the one that failed
            }
        }
    }

    /**
     * Bootstrap a new Concourse Server installation with the latest system
     * version, if necessary.
     * <p>
     * If this method detects that the Concourse Server installation is "fresh"
     * it will assign the latest system version without running any upgrade
     * tasks.
     * </p>
     * 
     * @return whatever the latest system version is after the affects, if any,
     *         of this method take affect; if the system is not fresh, calling
     *         this method has the same affect as calling
     *         {@link UpgradeTask#getCurrentSystemVersion()}.
     */
    private static int bootstrap() {
        String seal = ".douge";
        if(FileSystem.hasFile(seal)) {
            UpgradeTask theTask = null;
            // Go through the upgrade tasks and find the one with the largest
            // schema version.
            for (String pkg : pkgs) {
                Reflections reflections = new Reflections(pkg);
                Set<Class<? extends UpgradeTask>> classes = reflections
                        .getSubTypesOf(UpgradeTask.class);
                classes.addAll(
                        reflections.getSubTypesOf(SmartUpgradeTask.class));
                for (Class<? extends UpgradeTask> clazz : classes) {
                    UpgradeTask task = Reflection.newInstance(clazz);
                    if(theTask == null || task.version() > theTask.version()) {
                        theTask = task;
                    }
                }
            }
            UpgradeTask.setHomeCurrentSystemVersion(theTask.version());
            UpgradeTask.compareAndSetBufferCurrentSystemVersion(0,
                    theTask.version());
            UpgradeTask.compareAndSetDatabaseCurrentSystemVersion(0,
                    theTask.version());
            Logger.upgradeInfo(
                    "It appears that this is a fresh installation of "
                            + "Concourse Server, so the upgrade framework has been "
                            + "initialized with a system version of {}. Standby for "
                            + "detection of whether there are existing data files with "
                            + "a different system version; necessitating the possible "
                            + "application of upgrade tasks to make the system fully "
                            + "consistent.",
                    theTask.version());
            FileSystem.deleteFile(seal);
        }
        return getCurrentSystemVersion();
    }

}
