/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin.io;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.process.Callback;
import com.cinchapi.concourse.server.io.process.Forkable;
import com.cinchapi.concourse.server.io.process.NoOpCallback;
import com.cinchapi.concourse.server.io.process.ServerProcesses;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link InterProcessCommunication} that take advantage of the
 * server's
 * ability to fork processes from a local JVM.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public abstract class CrossProcessInterProcessCommunicationTest
        extends ConcourseBaseTest implements Serializable {

    public abstract InterProcessCommunication getInterProcessCommunication(
            String file);

    @Test
    public void testReadWrite() throws InterruptedException, IOException {
        final List<String> expected = Lists.newArrayList();
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            expected.add(Random.getString());
        }
        final String location = FileOps.tempFile();
        final String signal = FileOps.tempFile();
        Files.deleteIfExists(Paths.get(signal));
        Thread writer = new Thread(new Runnable() {

            @Override
            public void run() {
                InterProcessCommunication shared = getInterProcessCommunication(
                        location);
                for (String message : expected) {
                    shared.write(ByteBuffers.fromString(message));
                    Threads.sleep(TestData.getScaleCount());
                }
            }

        });

        Forkable<ArrayList<String>> reader = new Forkable<ArrayList<String>>() {

            @Override
            public ArrayList<String> call() throws Exception {
                ArrayList<String> actual = Lists.newArrayList();
                InterProcessCommunication shared = getInterProcessCommunication(
                        location);
                Files.createFile(Paths.get(signal));
                while (actual.size() < expected.size()) {
                    ByteBuffer data = shared.read();
                    String message = ByteBuffers.getString(data);
                    actual.add(message);
                    Threads.sleep(TestData.getScaleCount());
                }
                return actual;
            }

        };

        Callback<ArrayList<String>> callback = new NoOpCallback<ArrayList<String>>();
        ServerProcesses.fork(reader, callback);
        while (!Files.deleteIfExists(Paths.get(signal))) {
            continue;
        }
        writer.start();
        writer.join();
        Assert.assertEquals(expected, callback.getResult());
    }

}
