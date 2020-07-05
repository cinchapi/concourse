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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRevision;
import com.cinchapi.concourse.server.storage.db.legacy.Block;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;

/**
 * {@link UpgradeTask} to migrate data from Blocks to Segments.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_11_0_1 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Migrate Database data from Blocks to Segments";
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void doTask() {
        // TODO: need to remove segments if there is an error
        Environments
                .iterator(GlobalState.BUFFER_DIRECTORY,
                        GlobalState.DATABASE_DIRECTORY)
                .forEachRemaining(environment -> {
                    Path directory = Paths.get(GlobalState.DATABASE_DIRECTORY)
                            .resolve(environment);
                    Database db = new Database(directory);
                    db.start();
                    try {
                        Path cpb = directory.resolve("cpb");
                        Collection<Path> files = FileSystem.ls(cpb)
                                .filter(file -> file.toString()
                                        .endsWith(".blk"))
                                .map(Path::toFile)
                                .filter(file -> file.length() > 0)
                                .map(File::getAbsolutePath).map(Paths::get)
                                .collect(Collectors.toList());
                        for (Path file : files) {
                            Block<PrimaryKey, Text, Value> block = new Block(
                                    file, TableRevision.class);
                            for (Revision<PrimaryKey, Text, Value> revision : block) {
                                Write write = Reflection.newInstance(
                                        Write.class, revision.getType(),
                                        revision.getKey(), revision.getValue(),
                                        revision.getLocator(),
                                        revision.getVersion()); // (authorized)
                                db.accept(write);
                            }
                            db.triggerSync();
                        }
                    }
                    finally {
                        db.stop();
                    }
                });
    }

}
