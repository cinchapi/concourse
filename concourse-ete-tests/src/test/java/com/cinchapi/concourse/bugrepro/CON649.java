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
package com.cinchapi.concourse.bugrepro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit test to reproduce the issues described in CON-649.
 *
 * @author Jeff Nelson
 */
public class CON649 extends ClientServerTest {

    @Test
    public void repro()
            throws IOException, TTransportException, InterruptedException {
        List<Thread> clients = Lists.newArrayList();
        for (int i = 0; i < 1; ++i) {
            clients.add(new Thread(() -> {
                Concourse $client = server.connect();
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        $client.add(Random.getSimpleString(),
                                Random.getSimpleString(), Random.getLong());
                    }
                    catch (Exception e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }));
        }
        clients.forEach(Thread::start);
        Thread.sleep(10000);
        server.stop();
        Path db = server.getDatabaseDirectory().resolve("default");
        List<Path> directories = ImmutableList.of(db.resolve("cpb"),
                db.resolve("csb"), db.resolve("ctb"));
        Map<String, AtomicInteger> counts = Maps.newLinkedHashMap();
        directories.forEach(directory -> {
            try {
                Files.list(directory).forEach(file -> {
                    System.out.println(file);
                    String name = file.getFileName().toString().split("\\.")[0];
                    counts.computeIfAbsent(name, key -> new AtomicInteger(0))
                            .incrementAndGet();
                });
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
        Set<Integer> distinct = Sets.newHashSet();
        counts.forEach((path, count) -> {
            System.out.println(path + " = " + count);
            distinct.add(count.get());
        });
        System.out.println(counts.size());
        Assert.assertEquals(1, distinct.size());
        // server.start();
        // TODO: reconnect client and try to make a call and verify that there
        // is no exception

    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
