/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.ete.bugrepro;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests to validate that exported data can be imported as is.
 *
 * @author Jeff Nelson
 */
public class GH499 extends ClientServerTest {

    @Test
    public void reproGH_499() {
        String coverLetter = FileOps
                .read(Resources.getAbsolutePath("/cover-letter.txt"));
        Map<String, Object> data = ImmutableMap.of("name", "Jeff Nelson",
                "coverLetter", coverLetter, "age", ImmutableList.of(1, 2));
        long record = client.insert(data);
        Map<String, Set<Object>> expected = client.select(record);
        Path file = Paths.get(FileOps.tempFile());
        server.executeCli("export",
                "--file " + file.toString()
                        + " --username admin --password admin --port "
                        + server.getClientPort());
        FileOps.readLines(file.toString()).forEach(System.out::println);
        System.out.println(server.executeCli("import",
                Array.containing("--data", file.toString(), "-e", "test",
                        "--username", "admin", "--password", "admin", "--port",
                        Integer.toString(server.getClientPort()))));
        client = server.connect("admin", "admin", "test");
        Map<String, Set<Object>> actual = client
                .select(client.inventory().iterator().next());
        Assert.assertEquals(expected, actual);
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
