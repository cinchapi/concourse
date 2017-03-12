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
package com.cinchapi.concourse.server.storage;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Engine}.
 * 
 * @author Jeff Nelson
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
        final Engine engine = new Engine(loc + File.separator + "buffer",
                loc + File.separator + "db");
        engine.start();
        for (int i = 0; i < 1000; i++) {
            engine.accept(Write.add("foo", Convert.javaToThrift("bar"), i));
        }
        FileSystem.deleteDirectory(loc);
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                engine.find("foo", Operator.EQUALS,
                        Convert.javaToThrift("bar"));
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
        System.out.println(
                "[INFO] You can ignore the NoSuchFileException stack trace above");
    }

    @Test
    public void testNoBufferTransportBlockingIfWritesAreWithinThreshold() {
        String loc = TestData.DATA_DIR + File.separator + Time.now();
        final Engine engine = new Engine(loc + File.separator + "buffer",
                loc + File.separator + "db");
        Variables.register("now", Time.now());
        engine.start();
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
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
            Assert.assertTrue(engine
                    .find("count", Operator.EQUALS, Convert.javaToThrift(i))
                    .contains(Integer.valueOf(i).longValue()));
        }
    }

    @Test
    public void testBufferTransportBlockingIfWritesAreNotWithinThreshold() {
        String loc = TestData.DATA_DIR + File.separator + Time.now();
        final Engine engine = new Engine(loc + File.separator + "buffer",
                loc + File.separator + "db");
        engine.start();
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        Threads.sleep(
                Engine.BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS
                        + 30);
        engine.add(TestData.getSimpleString(), TestData.getTObject(),
                TestData.getLong());
        Assert.assertTrue(engine.bufferTransportThreadHasEverPaused.get());
        engine.stop();
        FileSystem.deleteDirectory(loc);
    }

    @Test
    public void testBrowseKeyIsSortedBetweenDatabaseAndBuffer() {
        Engine engine = (Engine) store;
        List<String> colleges = Lists.newArrayList("Boston College",
                "Yale University", "Harvard University");
        for (String college : colleges) {
            engine.destination.accept(Write.add("name",
                    Convert.javaToThrift(college), Time.now()));
        }
        engine.buffer.insert(
                Write.add("name", Convert.javaToThrift("jeffery"), Time.now()));
        Set<TObject> keys = engine.browse("name").keySet();
        Assert.assertEquals(Convert.javaToThrift("Boston College"),
                Iterables.get(keys, 0));
        Assert.assertEquals(Convert.javaToThrift("Harvard University"),
                Iterables.get(keys, 1));
        Assert.assertEquals(Convert.javaToThrift("jeffery"),
                Iterables.get(keys, 2));
        Assert.assertEquals(Convert.javaToThrift("Yale University"),
                Iterables.get(keys, 3));
    }

    @Test
    public void testBrowseRecordIsCorrectAfterRemoves() {
        Engine engine = (Engine) store;
        engine.add("name", Convert.javaToThrift("abc"), 1);
        engine.add("name", Convert.javaToThrift("xyz"), 2);
        engine.add("name", Convert.javaToThrift("abcd"), 3);
        engine.add("name", Convert.javaToThrift("abce"), 4);
        engine.remove("name", Convert.javaToThrift("xyz"), 2);
        Assert.assertTrue(engine.select(2).isEmpty()); // assert record
                                                       // presently has no data
        Assert.assertEquals(engine.getAllRecords(), Sets.<Long> newHashSet(
                new Long(1), new Long(2), new Long(3), new Long(4)));
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
                        engine.add(TestData.getSimpleString(),
                                TestData.getTObject(), TestData.getLong());
                    }

                }

            });
            thread.start();
            Threads.sleep((int) (1.2
                    * Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS)
                    + Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
            Assert.assertTrue(
                    engine.bufferTransportThreadHasEverAppearedHung.get());
            Threads.sleep(
                    (int) (Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS
                            * 1.2));
            Assert.assertTrue(
                    engine.bufferTransportThreadHasEverBeenRestarted.get());
            engine.stop();
            FileSystem.deleteDirectory(loc);
        }
        finally {
            done.set(true);
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = frequency;
            Engine.BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = threshold;
        }
    }

    @Test
    public void reproCON_239BrowseRecord() throws InterruptedException {
        final Engine engine = (Engine) store;
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            engine.add(Long.toString(Time.now()), Convert.javaToThrift(i), 1);
        }
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean go = new AtomicBoolean(false);
        Thread write = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!go.get()) {
                    continue; // ensure read goes first
                }
                while (!done.get()) {
                    if(!done.get()) {
                        engine.add(Long.toString(Time.now()),
                                Convert.javaToThrift("a"), 1);
                    }
                }
            }

        });
        final AtomicBoolean succeeded = new AtomicBoolean(true);
        Thread read = new Thread(new Runnable() {

            @Override
            public void run() {
                go.set(true);
                Map<String, Set<TObject>> data = engine.select(1);
                done.set(true);
                Map<String, Set<TObject>> data1 = engine.select(1);
                Variables.register("data_size", data.size());
                Variables.register("data1_size", data1.size());
                succeeded.set(data.size() == data1.size()
                        || data.size() == data1.size() - 1);
            }

        });

        read.start();
        write.start();
        read.join();
        write.join();
        Assert.assertTrue(succeeded.get());
    }

    @Test
    public void reproCON_239BrowseKey() throws InterruptedException {
        final Engine engine = (Engine) store;
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            engine.add("foo", Convert.javaToThrift(i), i);
        }
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean go = new AtomicBoolean(false);
        Thread write = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!go.get()) {
                    continue; // ensure read goes first
                }
                while (!done.get()) {
                    if(!done.get()) {
                        engine.add("foo",
                                Convert.javaToThrift(Long.toString(Time.now())),
                                Time.now());
                    }
                }
            }

        });
        final AtomicBoolean succeeded = new AtomicBoolean(true);
        Thread read = new Thread(new Runnable() {

            @Override
            public void run() {
                go.set(true);
                Map<TObject, Set<Long>> data = engine.browse("foo");
                done.set(true);
                Map<TObject, Set<Long>> data1 = engine.browse("foo");
                Variables.register("data_size", data.size());
                Variables.register("data1_size", data1.size());
                succeeded.set(data.size() == data1.size()
                        || data.size() == data1.size() - 1);
            }

        });

        read.start();
        write.start();
        read.join();
        write.join();
        Assert.assertTrue(succeeded.get());
    }

    @Test
    public void reproCON_516() {
        Engine engine = (Engine) store;
        Buffer buffer = (Buffer) engine.buffer;
        int count = 0;
        while (!(boolean) Reflection.call(buffer, "canTransport")) {
            add("name", Convert.javaToThrift("Jeff"), Time.now());
            count++;
        }
        buffer.transport(engine.destination);
        add("name", Convert.javaToThrift("Jeff"), Time.now());
        count++;
        Set<Long> matches = engine.find("name", Operator.EQUALS,
                Convert.javaToThrift("jeff"));
        Assert.assertEquals(count, matches.size());
    }

    @Test
    public void reproCON_239AuditRecord() throws InterruptedException {
        final Engine engine = (Engine) store;
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            engine.add(Long.toString(Time.now()), Convert.javaToThrift(i), 1);
        }
        engine.add("foo", Convert.javaToThrift("a"), 1);
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean go = new AtomicBoolean(false);
        Thread write = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!go.get()) {
                    continue; // ensure read goes first
                }
                while (!done.get()) {
                    if(!done.get()) {
                        engine.add("foo",
                                Convert.javaToThrift(Long.toString(Time.now())),
                                1);
                    }
                }
            }

        });
        final AtomicBoolean succeeded = new AtomicBoolean(true);
        Thread read = new Thread(new Runnable() {

            @Override
            public void run() {
                go.set(true);
                Map<Long, String> data = engine.audit(1);
                done.set(true);
                Map<Long, String> data1 = engine.audit(1);
                Variables.register("data_size", data.size());
                Variables.register("data1_size", data1.size());
                succeeded.set(data.size() == data1.size()
                        || data.size() == data1.size() - 1);
            }

        });

        read.start();
        write.start();
        read.join();
        write.join();
        Assert.assertTrue(succeeded.get());
    }

    // @Test
    // public void testAddThroughputDifferentKeysInRecord() throws
    // InterruptedException {
    // final Engine engine = (Engine) store;
    // final AtomicBoolean done = new AtomicBoolean(false);
    // Thread a = new Thread(new Runnable() {
    //
    // @Override
    // public void run() {
    // while (!done.get()) {
    // engine.add("foo", Convert.javaToThrift(Time.now()), 1);
    // }
    // }
    //
    // });
    // Thread b = new Thread(new Runnable(){
    //
    // @Override
    // public void run() {
    // while (!done.get()) {
    // engine.add("bar", Convert.javaToThrift(Time.now()), 1);
    // }
    // }
    //
    // });
    // a.start();
    // b.start();
    // TestData.sleep();
    // done.set(true);
    // a.join();
    // b.join();
    // System.out.println(engine.fetch("foo", 1).size());
    // System.out.println(engine.fetch("bar", 1).size());
    // }

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
        return new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        ((Engine) store).remove(key, value, record);

    }

}
