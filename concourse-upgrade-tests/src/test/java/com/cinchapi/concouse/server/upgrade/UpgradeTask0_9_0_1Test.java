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
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.BlockStats.Attribute;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;

/**
 * Unit tests to verify the validity of {@link UpgradeTask0_9_0_1}.
 *
 * @author Jeff Nelson
 */
public class UpgradeTask0_9_0_1Test extends UpgradeTest {

    private static final String environment1 = "env1";
    private static final String environment2 = "env2";

    @Override
    protected String getInitialServerVersion() {
        return "0.8.0";
    }

    @Override
    protected void preUpgradeActions() {
        Path directory1 = server.getDatabaseDirectory().resolve(environment1)
                .resolve("cpb");
        Path directory2 = server.getDatabaseDirectory().resolve(environment2)
                .resolve("cpb");
        Concourse client1 = Concourse.connect("localhost",
                server.getClientPort(), "admin", "admin", environment1);
        Concourse client2 = Concourse.connect("localhost",
                server.getClientPort(), "admin", "admin", environment2);
        client1.add("name", "jeff nelson", 17);
        try {
            int count = TestData.getScaleCount();
            Stream<Path> stream1 = null;
            Stream<Path> stream2 = null;
            try {
                while ((stream1 == null || stream2 == null)
                        || stream1.count() < count || stream2.count() < count) {
                    client1.add(Random.getSimpleString(), Random.getObject());
                    client2.add(Random.getSimpleString(), Random.getObject());
                    if(stream1 != null) {
                        stream1.close();
                    }
                    if(stream2 != null) {
                        stream2.close();
                    }
                    stream1 = Files.list(directory1);
                    stream2 = Files.list(directory2);
                }
            }
            finally {
                stream1.close();
                stream2.close();
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Test
    public void testPreviouslyExistingDataIsStillReadable() {
        Concourse client1 = Concourse.connect("localhost",
                server.getClientPort(), "admin", "admin", environment1);
        Assert.assertEquals("jeff nelson", client1.get("name", 17));
    }

    @Test
    public void testEachBlockHasStatsFile() {
        ImmutableList.of(environment1, environment2).forEach(environment -> {
            server.executeCli("debug", "list", "-e", environment)
                    .forEach(line -> {
                        String[] toks = line.split(Pattern.quote(")"));
                        if(toks.length > 1) {
                            Long id = Longs.tryParse(toks[1].trim());
                            if(id != null) {
                                ImmutableList.of("cpb", "csb", "ctb")
                                        .forEach(block -> {
                                            String file = server
                                                    .getDatabaseDirectory()
                                                    .resolve(environment)
                                                    .resolve(block)
                                                    .resolve(id + ".stts")
                                                    .toString();
                                            Assert.assertTrue(
                                                    FileSystem.hasFile(file));
                                        });
                            }
                        }
                    });
        });

    }

    @Test
    public void testEachBlockHasSchemaVersion() {
        ImmutableList.of(environment1, environment2).forEach(environment -> {
            Database database = new Database(server.getDatabaseDirectory()
                    .resolve(environment).toString());
            database.start();
            try {
                ImmutableList.of("cpb", "csb", "ctb").forEach(variable -> {
                    List<?> blocks = Reflection.get(variable, database);
                    blocks.forEach(block -> {
                        BlockStats stats = Reflection.get("stats", block);
                        Assert.assertNotNull(
                                stats.get(Attribute.SCHEMA_VERSION));
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
