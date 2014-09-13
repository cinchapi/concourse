/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.upgrade;

import java.util.Set;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.util.Logger;
import org.reflections.Reflections;

/**
 * The {@link Initializer} is responsible for setting the schema version during
 * a new installation.
 * 
 * @author jnelson
 */
public class Initializer {

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
                    if(theTask == null
                            || task.version() > theTask
                                    .version()) {
                        theTask = task;
                    }
                }
            }

            theTask.propagateSystemVersionUpdate();
            Logger.info("The system version has been set to {}",
                    theTask.version());
            System.exit(0);

        }
        finally {
            GlobalState.ENABLE_CONSOLE_LOGGING = enableConsoleLogging;
        }
    }

}
