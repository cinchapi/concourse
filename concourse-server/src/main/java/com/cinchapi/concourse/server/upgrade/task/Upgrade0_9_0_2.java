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

import java.util.concurrent.atomic.AtomicLong;

import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.server.upgrade.util.Storage;
import com.cinchapi.concourse.server.upgrade.util.Storage.Database;

/**
 * An {@link UpgradeTask} that populates the min/max revision version in each
 * existing {@link Block}.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_9_0_2 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Populate the min/max revision version in each existing database block";
    }

    @Override
    protected void doTask() {
        Storage.environments().forEach(environment -> {
            Database database = environment.database();
            database.start();
            try {
                database.blocks().forEach(block -> {
                    AtomicLong min = new AtomicLong(Long.MAX_VALUE);
                    AtomicLong max = new AtomicLong(Long.MIN_VALUE);
                    block.revisions().forEach(revision -> {
                        if(revision.getVersion() > max.get()) {
                            max.set(revision.getVersion());
                        }
                        if(revision.getVersion() < min.get()) {
                            min.set(revision.getVersion());
                        }
                    });
                    BlockStats stats = block.stats();
                    stats.put(Attribute.MIN_REVISION_VERSION, min.get());
                    stats.put(Attribute.MAX_REVISION_VERSION, max.get());
                    stats.sync();
                });
            }
            finally {
                database.stop();
            }
        });

    }

}
