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
package com.cinchapi.concourse.server.storage.db;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for {@link BlockStats}.
 *
 * @author Jeff Nelson
 */
public class BlockStatsTest {

    @Test
    public void testDataDoesNotPersistOnSync() {
        Path file = Paths.get(FileOps.tempFile());
        BlockStats stats = new BlockStats(file);
        String key = Random.getSimpleString();
        String value = Random.getSimpleString();
        stats.put(key, value);
        stats = new BlockStats(file);
        Assert.assertNull(stats.get(key));
    }

    @Test
    public void testDataPersistsOnSync() {
        Path file = Paths.get(FileOps.tempFile());
        BlockStats stats = new BlockStats(file);
        String key = Random.getSimpleString();
        String value = Random.getSimpleString();
        stats.put(key, value);
        stats.sync();
        stats = new BlockStats(file);
        Assert.assertEquals(value, stats.get(key));
    }
}
