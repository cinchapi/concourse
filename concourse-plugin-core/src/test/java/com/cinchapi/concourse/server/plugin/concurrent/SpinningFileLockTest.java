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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for {@link SpinningFileLock}.
 * 
 * @author Jeff Nelson
 */
public class SpinningFileLockTest {

    @Test
    public void testOverlappingLockSpinsInSameJvm()
            throws InterruptedException {
        FileChannel channel = randomFileChannel();
        CountDownLatch latch = new CountDownLatch(2);
        Thread a = new Thread(() -> {
            FileLock lock = FileLocks.lock(channel, 0, 10, false);
            Random.tinySleep();
            latch.countDown();
            FileLocks.release(lock);
        });
        Thread b = new Thread(() -> {
            FileLock lock = FileLocks.lock(channel, 3, 8, false);
            Random.tinySleep();
            latch.countDown();
            FileLocks.release(lock);
        });
        a.start();
        b.start();
        latch.await();
        Assert.assertTrue(true);
    }
    
    @Test
    public void testNonOverlappingDoesNotBlockInSameJvm(){
        FileChannel channel = randomFileChannel();
        FileLocks.lock(channel, 0, 1, false);
        FileLocks.lock(channel, 1, 1, false);
        Assert.assertTrue(true);
    }

    /**
     * Return a random {@link FileChannel} to use in tests.
     * 
     * @return a FileChannel
     */
    private static final FileChannel randomFileChannel() {
        Path path = Paths.get(FileOps.tempFile());
        try {
            return FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

}
