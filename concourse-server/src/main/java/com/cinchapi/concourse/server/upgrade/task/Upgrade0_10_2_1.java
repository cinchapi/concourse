/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.server.upgrade.util.Storage;
import com.cinchapi.concourse.server.upgrade.util.Storage.Database;
import com.cinchapi.concourse.util.Logger;

/**
 * {@link UpgradeTask} to re-index the Database. This is necessary for changes
 * in the way that BlockIndex composites are generated.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_10_2_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Reindex all database blocks";
    }

    @Override
    protected void doTask() {
        Storage.environments().forEach(environment -> {
            Database database = environment.database();
            database.start();
            try {
                database.blocks().forEach(block -> {
                    block.reindex();
                    Logger.info("Reindexed {}", block);
                });
            }
            finally {
                database.stop();
            }
        });
    }

}
