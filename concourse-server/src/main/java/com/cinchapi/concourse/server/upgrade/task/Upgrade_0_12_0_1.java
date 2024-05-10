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
package com.cinchapi.concourse.server.upgrade.task;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;

/**
 * Upgrade Task to reindex data to comply with changes in Version 0.12.
 *
 * @author Jeff Nelson
 */
public class Upgrade_0_12_0_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Reindex data";
    }

    @Override
    protected void doTask() {
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining(environment -> {
                    logInfoMessage("Reindexing environment {}", environment);
                    Path directory = Paths.get(GlobalState.DATABASE_DIRECTORY)
                            .resolve(environment);
                    Database db = new Database(directory);
                    db.start();
                    db.reindex();
                    db.stop();
                });
    }

    @Override
    protected void rollback() {}

}
