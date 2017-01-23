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
package com.cinchapi.concourse.server.plugin;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.io.InterProcessCommunication;
import com.cinchapi.concourse.server.plugin.io.MessageQueue;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Maps;

/**
 * Unit tests for {@link BackgroundThread} and {@link BackgroundExecutor}.
 * 
 * @author Jeff Nelson
 */
public class BackgroundThreadTest {

    @Test
    public void testBackgroundExecutorSetsEnvironmentCorrectly()
            throws InterruptedException {
        InterProcessCommunication outgoing = new MessageQueue();
        ConcurrentMap<AccessToken, RemoteMethodResponse> responses = Maps
                .newConcurrentMap();
        String environment1 = Random.getSimpleString();
        String environment2 = Random.getSimpleString();
        MockConcourseRuntime runtime = new MockConcourseRuntime();
        BackgroundExecutor executor = PluginExecutors
                .newCachedBackgroundExecutor(outgoing, responses);
        CountDownLatch latch = new CountDownLatch(2);
        final AtomicBoolean passed = new AtomicBoolean(true);
        executor.execute(environment1, () -> {
            try {
                Assert.assertEquals(environment1, runtime.environment());
                latch.countDown();
            }
            catch (AssertionError e) {
                passed.set(false);
                e.printStackTrace();
            }
        });
        executor.execute(environment2, () -> {
            try {
                Assert.assertEquals(environment2, runtime.environment());
                latch.countDown();
            }
            catch (AssertionError e) {
                passed.set(false);
                e.printStackTrace();
            }
        });
        latch.await();
        Assert.assertTrue(passed.get());
    }

    @Test
    public void testBackgroundExecutorHasCorrectInformation()
            throws InterruptedException {
        InterProcessCommunication outgoing = new MessageQueue();
        ConcurrentMap<AccessToken, RemoteMethodResponse> responses = Maps
                .newConcurrentMap();
        BackgroundExecutor executor = PluginExecutors
                .newCachedBackgroundExecutor(outgoing, responses);
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean passed = new AtomicBoolean(true);
        executor.execute(Random.getSimpleString(), () -> {
            InterProcessCommunication myOutgoing = ((BackgroundThread) Thread
                    .currentThread()).outgoing();
            ConcurrentMap<AccessToken, RemoteMethodResponse> myResponses = ((BackgroundThread) Thread
                    .currentThread()).responses();
            try {
                Assert.assertSame(outgoing, myOutgoing);
                Assert.assertSame(responses, myResponses);
            }
            catch (AssertionError e) {
                passed.set(false);
                e.printStackTrace();
            }
            latch.countDown();
        });

        latch.await();
        Assert.assertTrue(passed.get());
    }

}
