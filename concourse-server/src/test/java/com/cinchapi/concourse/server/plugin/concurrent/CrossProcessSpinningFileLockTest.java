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
package com.cinchapi.concourse.server.plugin.concurrent;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.State;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.process.Callback;
import com.cinchapi.concourse.server.io.process.Forkable;
import com.cinchapi.concourse.server.io.process.ServerProcesses;
import com.cinchapi.concourse.util.FileOps;
import com.google.common.collect.Lists;

/**
 * Unit tests for cross process performance of {@link SpinningFileLock}s.
 * 
 * @author Jeff Nelson
 */
public class CrossProcessSpinningFileLockTest implements Serializable {

    private static final long serialVersionUID = 1L;

    @Test
    public void testCrossProcessFileLocking()
            throws IOException, InterruptedException {
        String path = FileOps.tempFile();
        FileChannel channel = FileChannel.open(Paths.get(path),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        Forkable<ArrayList<String>> forkable = new Forkable<ArrayList<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public ArrayList<String> call() throws Exception {
                FileChannel channel = FileChannel.open(Paths.get(path),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLocks.lock(channel, 0, 10, false);
                Thread.sleep(5000);
                return Lists.newArrayList();
            }

        };
        AtomicBoolean hasResult = new AtomicBoolean(false);
        Callback<ArrayList<String>> callback = new Callback<ArrayList<String>>() {

            @Override
            public void onResult(ArrayList<String> result) {
                hasResult.set(true);
            }

        };
        ServerProcesses.fork(forkable, callback);
        Thread.sleep(1000);
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            FileLocks.lock(channel, 0, 10, false);
            latch.countDown();

        });
        t.start();
        while (t.getState() == State.RUNNABLE) {
            continue;
        }
        while (!hasResult.get()) {
            Assert.assertNotEquals(t.getState(), State.RUNNABLE);
        }
        latch.await();
        Assert.assertEquals(t.getState(), State.TERMINATED);
    }

}
