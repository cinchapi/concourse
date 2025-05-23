/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.transporter;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Tests for the {@link StreamingTransporter}.
 *
 * @author Jeff Nelson
 */
public class StreamingTransporterTest extends AbstractTransporterTest {

    @Test
    public void testNoBufferTransportBlockingIfWritesAreWithinThreshold() {
        StreamingTransporter transporter = Reflection.get("transporter",
                engine);
        Variables.register("now", Time.now());
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        engine.stop();
        Assert.assertFalse(
                transporter.bufferTransportThreadHasEverPaused.get());
    }

    @Test
    public void testBufferTransportBlockingIfWritesAreNotWithinThreshold() {
        StreamingTransporter transporter = Reflection.get("transporter",
                engine);
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        Threads.sleep(
                StreamingTransporter.BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS
                        + 30);
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        Assert.assertTrue(transporter.bufferTransportThreadHasEverPaused.get());
    }

    @Test
    public void testBufferTransportThreadWillRestartIfHung() {
        int frequency = StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS;
        int threshold = StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS;
        final AtomicBoolean done = new AtomicBoolean(false);
        try {
            engine.stop();
            StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = 100;
            StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = 500;
            int lag = 5000;
            engine.start();
            StreamingTransporter transporter = Reflection.get("transporter",
                    engine);
            transporter.bufferTransportThreadSleepInMs = StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS
                    + lag;
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (!done.get()) {
                        engine.add(TestData.getSimpleString(),
                                TestData.getTObject(), TestData.getLong());
                    }

                }

            });
            thread.start();
            Threads.sleep((int) (1.2
                    * StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS)
                    + StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
            while (!transporter.bufferTransportThreadHasEverAppearedHung
                    .get()) {
                System.out.println("Waiting to detect hung thread...");
                continue; // spin until the thread hang is detected
            }
            Assert.assertTrue(
                    transporter.bufferTransportThreadHasEverAppearedHung.get());
            Threads.sleep(
                    (int) (StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS
                            * 1.2));
            Assert.assertTrue(
                    transporter.bufferTransportThreadHasEverBeenRestarted
                            .get());
        }
        finally {
            done.set(true);
            StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = frequency;
            StreamingTransporter.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = threshold;
        }
    }

    @Override
    protected boolean enableBatchTransporter() {
        return false;
    }

}
