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

import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Checksums;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.ClientServerTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Unit test to ensure that Blocks are properly reindexed on upgrade after
 * change to the way in which lookup Composite keys are generated.
 *
 * @author Jeff Nelson
 */
public class UpgradeTask0_10_6_1Test extends UpgradeTest {

    String cpb;
    String csb;
    String ctb;

    Map<String, String> checksums = Maps.newHashMap();

    @Override
    protected String getInitialServerVersion() {
        return "0.10.5";
    }

    @Override
    protected void preUpgradeActions() {
        String env = "default";
        cpb = server.getDatabaseDirectory().resolve(env).resolve("cpb")
                .toString();
        csb = server.getDatabaseDirectory().resolve(env).resolve("cpb")
                .toString();
        ctb = server.getDatabaseDirectory().resolve(env).resolve("cpb")
                .toString();
        client.add("payRangeMax", 18, 1);
        while (checksums.isEmpty()) {
            ClientServerTests.insertRandomData(server, env);
            ImmutableList.of(cpb, csb, ctb).forEach(group -> {
                Iterable<String> files = () -> FileSystem
                        .fileOnlyIterator(group);
                StreamSupport.stream(files.spliterator(), false)
                        .filter(p -> p.endsWith(".fltr") || p.endsWith(".indx")
                                || p.endsWith("stts") || p.endsWith("blk"))
                        .map(Paths::get).forEach(path -> {
                            checksums.put(path.toString(),
                                    Checksums.generate(path));
                        });
            });

        }
    }

    @Test
    public void testOnlyIndxFilesHaveChanged() {
        Assert.assertTrue(client.verify("payRangeMax", 18, 1));
        ImmutableList.of(cpb, csb, ctb).forEach(group -> {
            FileSystem.fileOnlyIterator(group).forEachRemaining(file -> {
                String actual = Checksums.generate(Paths.get(file));
                String expected = checksums.get(file);
                if(expected != null) {
                    if(file.endsWith(".indx")) {
                        Assert.assertNotEquals(expected, actual);
                    }
                    else {
                        Assert.assertEquals(expected, actual);
                    }
                }
            });
        });
    }

}
