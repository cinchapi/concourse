/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.io.File;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.server.concurrent.Threads;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.db.Database;
import org.cinchapi.concourse.server.storage.temp.Buffer;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Random;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Unit tests for {@link Engine}.
 * 
 * @author jnelson
 */
public class EngineTest extends BufferedStoreTest {

    private String directory;

    @Rule
    public TestWatcher w = new TestWatcher() {
        @Override
        protected void starting(Description desc) {
            store.stop(); // Stop the engine so that data isn't transported in
                          // the middle of a test.
        }
    };

    @Test(timeout = 30000)
    public void testNoDeadlockIfTransportExceptionOccurs()
            throws InterruptedException {
        // NOTE: This test is EXPECTED to print a NoSuchFileException
        // stacktrace. It can be ignored.
        String loc = TestData.DATA_DIR + File.separator + Time.now();
        final Engine engine = new Engine(loc + File.separator + "buffer", loc
                + File.separator + "db");
        engine.start();
        for (int i = 0; i < 1000; i++) {
            engine.accept(Write.add("foo", Convert.javaToThrift("bar"), i));
        }
        FileSystem.deleteDirectory(loc);
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                engine.find("foo", Operator.EQUALS, Convert.javaToThrift("bar"));
            }

        });
        Thread.sleep(2000); // this is an arbitrary amount. In 2 seconds, at
                            // least one page should have transported...
        a.start();
        a.join();
        engine.stop();
        Assert.assertTrue(true); // if we reach here, this means that the Engine
                                 // was able to break out of the transport
                                 // exception
        System.out
                .println("[INFO] You can ignore the NoSuchFileException stack trace above");
    }

    @Test
    public void testNoBufferTransportBlockingIfWritesAreWithinThreshold() {
        String loc = TestData.DATA_DIR + File.separator + Time.now();
        final Engine engine = new Engine(loc + File.separator + "buffer", loc
                + File.separator + "db");
        Variables.register("now", Time.now());
        engine.start();
        engine.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
        engine.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
        engine.stop();
        Assert.assertFalse(engine.bufferTransportThreadHasEverPaused.get());
        FileSystem.deleteDirectory(loc);
    }

    @Test
    public void testNoDuplicateDataIfUnexpectedShutdownOccurs()
            throws Exception {
        Engine engine = (Engine) store;
        Buffer buffer = (Buffer) engine.buffer;
        Database db = (Database) engine.destination;
        Method method = buffer.getClass().getDeclaredMethod("canTransport");
        method.setAccessible(true);
        int count = 0;
        while (!(boolean) method.invoke(buffer)) {
            engine.add("count", Convert.javaToThrift(count),
                    Integer.valueOf(count).longValue());
            count++;
        }
        for (int i = 0; i < count - 2; i++) { // leave one write on the page so
                                              // buffer doesn't automatically
                                              // call db.triggerSync()
            buffer.transport(db);
        }
        db.triggerSync();
        engine = new Engine(buffer.getBackingStore(), db.getBackingStore());
        engine.start(); // Simulate unexpected shutdown by "restarting" the
                        // Engine
        while ((boolean) method.invoke(engine.buffer)) { // wait until the first
                                                         // page in the buffer
                                                         // (which contains the
                                                         // same data that was
                                                         // previously
                                                         // transported) is done
                                                         // transporting again
            Random.sleep();
        }
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(engine.find("count", Operator.EQUALS,
                    Convert.javaToThrift(i)).contains(
                    Integer.valueOf(i).longValue()));
        }
    }

    @Test
    public void testBufferTransportBlockingIfWritesAreNotWithinThreshold() {
        String loc = TestData.DATA_DIR + File.separator + Time.now();
        final Engine engine = new Engine(loc + File.separator + "buffer", loc
                + File.separator + "db");
        engine.start();
        engine.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
        Threads.sleep(Engine.BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS + 10);
        engine.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
        Assert.assertTrue(engine.bufferTransportThreadHasEverPaused.get());
        engine.stop();
        FileSystem.deleteDirectory(loc);
    }

    @Test
    public void testBufferTransportThreadWillRestartIfHung() {
        int frequency = Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS;
        int threshold = Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS;
        final AtomicBoolean done = new AtomicBoolean(false);
        try {
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = 100;
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = 500;
            int lag = 5000;
            String loc = TestData.DATA_DIR + File.separator + Time.now();
            final Engine engine = new Engine(loc + File.separator + "buffer",
                    loc + File.separator + "db");
            engine.bufferTransportThreadSleepInMs = Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS
                    + lag;
            engine.start();
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (!done.get()) {
                        engine.add(TestData.getString(), TestData.getTObject(),
                                TestData.getLong());
                    }

                }

            });
            thread.start();
            Threads.sleep(Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS
                    + Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
            Assert.assertTrue(engine.bufferTransportThreadHasEverAppearedHung
                    .get());
            Threads.sleep(Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS);
            Assert.assertTrue(engine.bufferTransportThreadHasEverBeenRestarted
                    .get());
            engine.stop();
            FileSystem.deleteDirectory(loc);
        }
        finally {
            done.set(true);
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = frequency;
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = threshold;
        }
    }

    @Override
    protected void add(String key, TObject value, long record) {
        ((Engine) store).add(key, value, record);
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(directory);

    }

    @Override
    protected Store getStore() {
        directory = TestData.DATA_DIR + File.separator + Time.now();
        return new Engine(directory + File.separator + "buffer", directory
                + File.separator + "database");
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        ((Engine) store).remove(key, value, record);

    }

}
