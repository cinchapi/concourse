/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade;

import java.util.Set;

import org.reflections.Reflections;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.util.Logger;

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
        // the Initializer runs in a different JVM that the normal
        // ConcourseServer process.
        boolean enableConsoleLogging = GlobalState.ENABLE_CONSOLE_LOGGING;
        GlobalState.ENABLE_CONSOLE_LOGGING = true;
        try {
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
                    UpgradeTask task = clazz.newInstance();
                    if(theTask == null || task.version() > theTask.version()) {
                        theTask = task;
                    }
                }
            }
            UpgradeTask.setCurrentSystemVersion(theTask.version());
            Logger.info("The system version has been set to {}",
                    theTask.version());
            System.exit(0);

        }
        finally {
            GlobalState.ENABLE_CONSOLE_LOGGING = enableConsoleLogging;
        }
    }

}
