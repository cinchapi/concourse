/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.util.List;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;
import com.google.common.collect.ImmutableList;

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
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining(environment -> {
                    Database database = new Database(FileSystem.makePath(
                            GlobalState.DATABASE_DIRECTORY, environment));
                    database.start();
                    long schemaVersion = Reflection.getStatic("SCHEMA_VERSION",
                            Reflection.getClassCasted(
                                    "com.cinchapi.concourse.server.storage.db.Block"));
                    // Go through all the blocks and set the schema version
                    ImmutableList.of("cpb", "csb", "ctb").forEach(variable -> {
                        List<?> list = Reflection.get(variable, database);
                        list.forEach(block -> {
                            BlockStats stats = Reflection.get("stats", block);
                            stats.put(Attribute.SCHEMA_VERSION, schemaVersion);
                            stats.sync();
                        });
                    });
                });
    }

}
