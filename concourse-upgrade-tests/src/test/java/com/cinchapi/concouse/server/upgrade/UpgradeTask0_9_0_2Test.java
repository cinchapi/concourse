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
package com.cinchapi.concouse.server.upgrade;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.ClientServerTests;
import com.google.common.collect.ImmutableList;

/**
 * Unit test for {@link Upgrade0_9_0_2}.
 *
 * @author Jeff Nelson
 */
public class UpgradeTask0_9_0_2Test extends UpgradeTest {

    private static final String environment1 = "env1";
    private static final String environment2 = "env2";

    @Override
    protected String getInitialServerVersion() {
        return "0.8.0";
    }

    @Override
    protected void preUpgradeActions() {
        ClientServerTests.insertRandomData(server, environment1, environment2);
    }

    @Test
    public void testAllBlocksHaveMinMaxVersionSet() {
        ImmutableList.of(environment1, environment2).forEach(environment -> {
            Database database = new Database(server.getDatabaseDirectory()
                    .resolve(environment).toString());
            database.start();
            try {
                ImmutableList.of("cpb", "csb", "ctb").forEach(variable -> {
                    List<?> blocks = Reflection.get(variable, database);
                    blocks.forEach(block -> {
                        if(Reflection.<Integer> call(block, "size") > 0) {
                            BlockStats stats = Reflection.get("stats", block);
                            Assert.assertNotNull(
                                    stats.get(Attribute.MIN_REVISION_VERSION));
                            Assert.assertNotNull(
                                    stats.get(Attribute.MAX_REVISION_VERSION));
                        }

                    });
                });
            }
            finally {
                database.stop();
            }

        });
    }

    @Test
    public void testNoExtraneousStatsFiles() {
        AtomicInteger blkFiles = new AtomicInteger(0);
        AtomicInteger sttsFiles = new AtomicInteger(0);
        ImmutableList.of("default", environment1, environment2)
                .forEach(environment -> {
                    ImmutableList.of("cpb", "csb", "ctb").forEach(variable -> {
                        try {
                            Files.list(server.getDatabaseDirectory()
                                    .resolve(environment).resolve(variable))
                                    .forEach(file -> {
                                        if(file.toString().endsWith("blk")) {
                                            blkFiles.incrementAndGet();
                                        }
                                        else if(file.toString()
                                                .endsWith("stts")) {
                                            sttsFiles.incrementAndGet();
                                        }
                                    });
                        }
                        catch (IOException e) {
                            throw CheckedExceptions.throwAsRuntimeException(e);
                        }
                    });
                });
        Assert.assertEquals(blkFiles.get(), sttsFiles.get());
    }

}
