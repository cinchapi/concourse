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
package org.cinchapi.concourse.server.upgrade.task;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.upgrade.UpgradeTask;
import org.cinchapi.concourse.util.Environments;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * Migrate legacy (e.g. pre-environment) data to the default environment.
 * 
 * @author jnelson
 */
public class Upgrade2 extends UpgradeTask {

    @Override
    public String getDescription() {
        return "Migrating pre-0.4 data to default environment";
    }

    @Override
    public int getSchemaVersion() {
        return 2;
    }

    @Override
    protected void doTask() {
        // Duplicate check that is done in ConcourseServer constructor to make
        // sure the sanitized default environment won't be an empty string
        String defaultEnv = Environments
                .sanitize(GlobalState.DEFAULT_ENVIRONMENT);
        Preconditions.checkState(!Strings.isNullOrEmpty(defaultEnv),
                "Cannot initialize "
                        + "Concourse Server with a default environment of "
                        + "'%s'. Please use a default environment name that "
                        + "contains only alphanumeric characters.",
                GlobalState.DEFAULT_ENVIRONMENT);

        String defaultBufferEnv = GlobalState.BUFFER_DIRECTORY + File.separator
                + GlobalState.DEFAULT_ENVIRONMENT;
        String defaultDbEnv = GlobalState.DATABASE_DIRECTORY + File.separator
                + GlobalState.DEFAULT_ENVIRONMENT;

        // Check to make sure that the default environment does yet not exist
        // (and is therefore empty) because otherwise migrating data to it could
        // lead to undefined and inconsistent results.
        String err = "Cannot migrate to the default %s environment "
                + "because it already contains data. Please change the "
                + "`default_environment` preference in concourse.prefs to "
                + "complete this task.";
        Preconditions.checkState(!FileSystem.hasDir(defaultBufferEnv), err,
                "buffer");
        Preconditions.checkState(!FileSystem.hasDir(defaultDbEnv), err,
                "database");

        try { // NOTE: when moving data, use path#endsWith() for directories and
              // path#toString()#endsWith() for files.

            // Migrate the legacy data in the buffer
            FileSystem.mkdirs(defaultBufferEnv);
            for (Path path : Files.newDirectoryStream(Paths
                    .get(GlobalState.BUFFER_DIRECTORY))) {

                if(path.toString().endsWith(".buf") || path.endsWith("txn")) {
                    File src = new File(path.toString());
                    File dest = new File(defaultBufferEnv + File.separator
                            + path.getFileName().toString());
                    src.renameTo(dest);
                    logInfoMessage("Moved {} to {}", src, dest);
                }
            }

            // Migrating the legacy data in the db
            FileSystem.mkdirs(defaultDbEnv);
            for (Path path : Files.newDirectoryStream(Paths
                    .get(GlobalState.DATABASE_DIRECTORY))) {
                if(path.endsWith("cpb") || path.endsWith("csb")
                        || path.endsWith("ctb")) {
                    File src = new File(path.toString());
                    File dest = new File(defaultDbEnv + File.separator
                            + path.getFileName().toString());
                    src.renameTo(dest);
                    logInfoMessage("Moved {} to {}", src, dest);
                }
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }
}
