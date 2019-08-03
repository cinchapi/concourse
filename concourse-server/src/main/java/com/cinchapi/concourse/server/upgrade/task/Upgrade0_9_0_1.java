/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.server.upgrade.util.Storage;
import com.cinchapi.concourse.server.upgrade.util.Storage.Block;
import com.cinchapi.concourse.server.upgrade.util.Storage.Database;

/**
 * An {@link UpgradeTask} to initialize the {@link Block#Stats}.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_9_0_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Generate initial block stats";
    }

    @Override
    protected void doTask() {
        Storage.environments().forEach(environment -> {
            Database database = environment.database();
            database.start();
            try {
                long schemaVersion = Block.SCHEMA_VERSION;
                database.blocks().forEach(block -> {
                    BlockStats stats = block.stats();
                    stats.put(Attribute.SCHEMA_VERSION, schemaVersion);
                    stats.sync();
                });
            }
            finally {
                database.stop();
            }
        });
    }

}
