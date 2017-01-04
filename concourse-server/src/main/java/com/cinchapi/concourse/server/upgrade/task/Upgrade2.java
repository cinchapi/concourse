/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade.task;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.upgrade.UpgradeTask;
import com.cinchapi.concourse.util.Environments;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * Migrate legacy (e.g. pre-environment) data to the default environment.
 * 
 * @author Jeff Nelson
 */
public class Upgrade2 extends UpgradeTask {

    @Override
    public String getDescription() {
        return "Migrating pre-0.4 data to default environment";
    }

    @Override
    public int version() {
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
