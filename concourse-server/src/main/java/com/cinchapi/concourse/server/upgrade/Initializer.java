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

import java.util.Set;

import org.reflections.Reflections;

import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Reflection;

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
     * Initialize the upgrade framework, if necessary.
     * <p>
     * Look at all the {@link UpgradeTask upgrade tasks} on the classpath
     * (without running any of them) and set the
     * {@link UpgradeTask#setCurrentSystemVersion(int) current system
     * version} to the largest seen.
     * </p>
     */
    public static void run() {
        if(shouldRun()) {
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
        }
    }

    /**
     * Return {@code true} if the {@link UpgradeTask#getCurrentSystemVersion()
     * current system version} is equal to {@code 0}, which implies that this is
     * a new Concourse Server installation and the {@link Initializer} should
     * run.
     * 
     * @return {@code true} if {@link #run()} should execute initialization
     *         logic
     */
    protected static boolean shouldRun() { // visible for testing
        return UpgradeTask.getCurrentSystemVersion() == 0;
    }

}
