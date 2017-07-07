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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.lang.Thread.State;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.google.common.base.Throwables;

/**
 * Unit tests for {@link FileOps}.
 * 
 * @author Jeff Nelson
 */
public class FileOpsTest extends ConcourseBaseTest {

    @Test(timeout = 10000)
    public void testAwaitChange() throws InterruptedException {
        String file = FileOps.tempFile(FileOps.tempDir("con"), "foo", ".test");
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            FileOps.awaitChange(file);
            latch.countDown();
        });
        t.start();
        AtomicBoolean done = new AtomicBoolean(false);
        Thread t2 = new Thread(() -> {
            while (!done.get()) {
                // There is an arbitrary and uncontrollable delay between the
                // time that the path Thread t starting and the
                // FileOps#awaitChange method actually registering the path so
                // we just keep writing to the file so that, eventually, one of
                // the changes will be caught
                FileOps.write(Random.getSimpleString(), file);
            }
        });
        t2.start();
        latch.await();
        Assert.assertTrue(true);
        done.set(true);
    }

    @Test(timeout = 5000)
    public void testAwaitChangeDoesNotRegisterPathMoreThanOnce()
            throws InterruptedException {
        // NOTE: Do not change this test to use a CountDownLatch or some other
        // precise way of measuring when the threads have completed because
        // change notifications are subject to arbitrary delays of the
        // underlying file system. Furthermore, this test is designed to test
        // registration and not the accuracy of the change notifications.
        String file = FileOps.tempFile();
        Thread t1 = new Thread(() -> {
            FileOps.awaitChange(file);
        });
        t1.setDaemon(true);
        Thread t2 = new Thread(() -> {
            FileOps.awaitChange(file);

        });
        t2.setDaemon(true);
        Assert.assertEquals(0, FileOps.REGISTERED_WATCHER_PATHS.size());
        t1.start();
        long start = System.nanoTime();
        while (FileOps.REGISTERED_WATCHER_PATHS.size() < 1) {
            continue;
        }
        long elapsed = System.nanoTime() - start;
        t2.start();
        TimeUnit.NANOSECONDS.sleep(elapsed * 10); // sleep for 10x how long it
                                                  // took to register the path
                                                  // so that we have a high
                                                  // degree of confidence that
                                                  // the second thread has
                                                  // finished
        FileOps.write("a", file);
        Assert.assertEquals(1, FileOps.REGISTERED_WATCHER_PATHS.size());
    }

    @Test(timeout = 5000)
    public void testRegisterDifferentPaths() {
        String file1 = FileOps.tempFile(FileOps.tempDir("con"), null, null);
        String file2 = FileOps.tempFile(FileOps.tempDir("con"), null, null);
        Thread t1 = new Thread(() -> {
            FileOps.awaitChange(file1);
        });
        t1.setDaemon(true);
        Thread t2 = new Thread(() -> {
            FileOps.awaitChange(file2);

        });
        t2.setDaemon(true);
        t1.start();
        t2.start();
        while (FileOps.REGISTERED_WATCHER_PATHS.size() < 2) {
            continue;
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testAwaitChangeNeverMissesUpdate()
            throws InterruptedException, IOException {
        String file = FileOps.tempFile();
        CountDownLatch testSignaler = new CountDownLatch(1);
        CountDownLatch writeSignaler = new CountDownLatch(1);
        Thread t1 = new Thread(() -> { // ensure the parent path gets registered
            Thread parentThread = Thread.currentThread();
            Thread raceConditionDetector = new Thread(() -> {
                while (parentThread.getState() == State.RUNNABLE) {
                    continue;
                }
                try {
                    if(Files.size(Paths.get(file)) > 0) {
                        FileOps.touch(file);
                    }
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            });
            raceConditionDetector.setDaemon(true);
            raceConditionDetector.start();
            writeSignaler.countDown();
            FileOps.awaitChange(file);
            testSignaler.countDown();
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            try {
                writeSignaler.await();
                FileOps.write("foo", file);            
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t2.start();
        testSignaler.await();
        CountDownLatch startable = new CountDownLatch(2);
        Thread t3 = new Thread(() -> {
            startable.countDown();
            try {
                startable.await();
                FileOps.awaitChange(file);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        Thread t4 = new Thread(() -> {
            startable.countDown();
            try {
                startable.await();
                FileOps.write("foo", file);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t3.start();
        t4.start();
        t3.join();
        t4.join();
    }

}
