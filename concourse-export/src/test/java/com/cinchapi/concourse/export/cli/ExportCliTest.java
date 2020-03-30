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
package com.cinchapi.concourse.export.cli;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link ExportCli}.
 *
 * @author Jeff Nelson
 */
public class ExportCliTest extends ClientServerTest {

    /**
     * Transform the {@code args} into a complete set arguments to use in a CLI
     * test.
     * 
     * @param args
     * @return the CLI args
     */
    private String[] generateCliArgs(String... args) {
        args = AnyObjects.split(args, ' ').toArray(Array.containing());
        ArrayBuilder<String> ab = ArrayBuilder.builder();
        ab.add("--username");
        ab.add("admin");
        ab.add("--password");
        ab.add("admin");
        ab.add("--port");
        ab.add(String.valueOf(server.getClientPort()));
        ab.add(args);
        return ab.build();
    }

    @Test
    public void testParseRecordsArgs() {
        String[] args = generateCliArgs(
                "-r 1 2 3 -r 4,5, 6,7 -r 8 -r 9 10 -r 11");
        ExportCli cli = new ExportCli(args);
        Assert.assertEquals(
                ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
                cli.records);
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
