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
package com.cinchapi.concourse.server.upgrade.task;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;

/**
 * Upgrade task to generate the initial Block stats.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_7_0_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Generate initial block stats";
    }

    @Override
    protected void doTask() {
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining((environment) -> {
                    // Load up the environment's database, which will
                    // instantiate all the Blocks and cause them to self-upgrade
                    // with the initial stats
                    Database db = new Database(FileSystem.makePath(
                            GlobalState.DATABASE_DIRECTORY, environment));
                    db.start();
                    db.stop();
                });

    }

}
