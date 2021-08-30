/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRevision;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.format.StorageFormatV2;
import com.cinchapi.concourse.server.storage.format.StorageFormatV2.Block;
import com.cinchapi.concourse.server.storage.format.StorageFormatV3;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;

/**
 * {@link UpgradeTask} to migrate data from Blocks to Segments.
 * <p>
 * Additionally, in version 0.11.0, the logic for generating {@link Composite
 * Composites} was changed, which affects existing bloom filters and index (e.g.
 * manifest) files. The overall transfer process will reindex all data, so the
 * necessary upgrades for those files is handled.
 * </p>
 *
 * @author Jeff Nelson
 */
public class Upgrade0_11_0_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Migrate Database data from Blocks to Segments";
    }

    @Override
    protected void doTask() {
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining(environment -> {
                    logInfoMessage(
                            "Upgrading Storage Format v2 data files to Storage Format v3 in environment {}",
                            environment);
                    Path directory = Paths.get(GlobalState.DATABASE_DIRECTORY)
                            .resolve(environment);
                    Database db = new Database(directory);
                    db.start();
                    try {
                        Path cpb = directory.resolve("cpb");
                        Iterable<Block<PrimaryKey, Text, Value>> blocks = StorageFormatV2
                                .load(cpb, TableRevision.class);
                        for (Block<PrimaryKey, Text, Value> block : blocks) {
                            for (Revision<PrimaryKey, Text, Value> revision : block) {
                                Write write = Reflection.newInstance(
                                        Write.class, revision.getType(),
                                        revision.getKey(), revision.getValue(),
                                        revision.getLocator(),
                                        revision.getVersion()); // (authorized)
                                db.accept(write);
                            }
                            db.sync();
                            logInfoMessage(
                                    "Finished transferring v2 data Block {} to v3 Segment format",
                                    block.getId());
                        }
                    }
                    finally {
                        db.stop();
                    }
                });
    }

    @Override
    protected void rollback() {
        logErrorMessage("Deleting Segment files");
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining(environment -> {
                    Path directory = Paths.get(GlobalState.DATABASE_DIRECTORY)
                            .resolve(environment).resolve("segments");
                    Iterable<Segment> segments = StorageFormatV3
                            .load(directory);
                    for (Segment segment : segments) {
                        segment.discard();
                    }
                });

    }

}
