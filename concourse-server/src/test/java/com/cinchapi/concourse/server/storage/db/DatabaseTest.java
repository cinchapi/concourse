/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.io.File;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.StoreTest;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link com.cinchapi.concourse.server.storage.db.Database}.
 *
 * @author Jeff Nelson
 */
public class DatabaseTest extends StoreTest {

    private String current;

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAllRecords() {
        Database db = (Database) store;
        db.getAllRecords();
    }

    @Test
    public void testDatabaseRemovesDuplicateSegmentsOnStartup() {
        Database db = (Database) store;
        int expected = TestData.getScaleCount();
        for (int i = 0; i < expected; ++i) {
            db.accept(Write.add(TestData.getString(), TestData.getTObject(),
                    TestData.getLong()));
            db.sync();
        }
        List<Segment> segments = Reflection.get("segments", db);
        Assert.assertEquals(expected + 1, segments.size()); // size includes
                                                            // seg0
        db.stop();
        AtomicInteger duplicates = new AtomicInteger(0);
        FileSystem.ls(Paths.get(current).resolve("segments")).forEach(file -> {
            if(Random.getInt() % 3 == 0) {
                FileSystem.copyBytes(file.toString(), file.getParent()
                        .resolve(UUID.randomUUID() + ".seg").toString());
                duplicates.incrementAndGet();
            }
        });
        db = new Database(db.getBackingStore()); // simulate server restart
        db.start();
        segments = Reflection.get("segments", db);
        Assert.assertEquals(expected + 1, segments.size()); // size includes
                                                            // seg0
        System.out.println("The database discarded " + duplicates.get()
                + " overlapping segments");
    }

    @Test
    public void testDatabaseAppendsToCachedPartialPrimaryRecords() {
        Database db = (Database) store;
        String key = TestData.getString();
        long record = TestData.getLong();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            db.accept(Write.add(key, Convert.javaToThrift(i), record));
        }
        db.select(key, record);
        int increase = TestData.getScaleCount();
        db.accept(
                Write.add(key, Convert.javaToThrift(count * increase), record));
        Assert.assertTrue(db.select(key, record)
                .contains(Convert.javaToThrift(count * increase)));
    }

    @Test
    public void testDatabaseAppendsToCachedSecondaryRecords() {
        Database db = (Database) store;
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            db.accept(Write.add(key, value, i));
        }
        db.find(key, Operator.EQUALS, value);
        int increase = TestData.getScaleCount();
        db.accept(Write.add(key, value, count * increase));
        Assert.assertTrue(db.find(key, Operator.EQUALS, value)
                .contains((long) count * increase));
    }

    @Test
    public void testSearchIndexMultipleEnvironmentsConcurrently() {
        List<Database> dbs = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            Database db = getStore();
            db.start();
            dbs.add(db);
        }
        String key = "test";
        String query = "son i";
        TObject value = Convert.javaToThrift("Jeff Nelson is the CEO");
        int count = 100;
        Set<Long> expected = Sets.newLinkedHashSet();
        Consumer<Database> func = db -> {
            for (int i = 0; i < count; ++i) {
                expected.add((long) i);
                Write write = Write.add("test", value, i);
                db.accept(write);
            }
        };
        for (Database db : dbs) {
            func.accept(db);
        }
        for (Database db : dbs) {
            Set<Long> actual = db.search(key, query);
            Assert.assertTrue(!actual.isEmpty());
            Assert.assertEquals(expected, actual);
        }
        for (Database db : dbs) {
            db.stop();
            if(!current.contentEquals(db.getBackingStore())) {
                FileSystem.deleteDirectory(db.getBackingStore());
            }
        }
    }

    @Test
    public void testLoadPrimaryRecordUsesFullRecordIfItMemory() {
        Database db = (Database) store;
        String a = "a";
        String b = "b";
        TObject value = Convert.javaToThrift(1);
        long record = 1;
        db.accept(Write.add(a, value, record));
        db.accept(Write.add(b, value, record));
        db.sync();
        db.stop();
        db = new Database(db.getBackingStore()); // TODO: cannot stop/start same
                                                 // Database instance because
                                                 // state isn't reset...
        db.start();
        TableRecord rec = Reflection.call(db, "getTableRecord",
                Identifier.of(record), Text.wrap(a)); // (authorized)
        Assert.assertTrue(rec.isPartial());
        db.select(record);
        rec = Reflection.call(db, "getTableRecord", Identifier.of(record),
                Text.wrap(b)); // (authorized)
        Assert.assertFalse(rec.isPartial());
    }

    @Test
    public void testGatherVsSelectBenchmark() {
        java.util.Random rand = new java.util.Random();
        Database store = (Database) this.store;
        List<Long> records = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount() * 100; ++i) {
            records.add(Time.now());
        }
        List<String> keys = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount() * 10; ++i) {
            keys.add(Random.getSimpleString());
        }
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            String key = keys.get(rand.nextInt(keys.size()));
            long record = records.get(rand.nextInt(records.size()));
            TObject value = Convert.javaToThrift(Random.getObject());
            add(key, value, record);
            if(rand.nextInt() % 6 == 0) {
                remove(key, value, record);
            }
        }
        Database $store = store;
        Benchmark select = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                for (long record : records) {
                    for (String key : keys) {
                        $store.select(key, record);
                    }
                }
            }

        };
        Benchmark gather = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                for (long record : records) {
                    for (String key : keys) {
                        $store.gather(key, record);
                    }
                }
            }

        };
        double selectTime = select.run(1);
        double gatherTime = gather.run(1);
        System.out.println("Select took " + selectTime + " ms and gather took "
                + gatherTime + " ms");
        Assert.assertTrue(gatherTime <= selectTime);
    }

    @Test
    public void testVerify() {
        add("name", Convert.javaToThrift("jeff"), 1);
        store.stop();
        ((Database) store).sync();
        store.start();
        Assert.assertTrue(
                store.verify("name", Convert.javaToThrift("jeff"), 1));
    }

    @Test
    public void testGetIndexRecordReproCON_674() {
        add("iq", Convert.javaToThrift("u"), 1605548010968002L);
        store.stop();
        ((Database) store).sync();
        store.start();
        Assert.assertTrue(store.browse("iqu").isEmpty());
    }

    @Test
    public void testSearchMultiValuedAfterRemove() {
        add("name", Convert.javaToThrift("jeff"), 1L);
        add("name", Convert.javaToThrift("jeffery"), 1L);
        Set<Long> actual = store.search("name", "jeff");
        Assert.assertEquals(ImmutableSet.of(1L), actual);
        remove("name", Convert.javaToThrift("jeff"), 1L);
        actual = store.search("name", "jeff");
        Assert.assertEquals(ImmutableSet.of(1L), actual);
    }

    @Test
    public void testBackgroundManifestLoadConcurrency()
            throws InterruptedException {
        Database db = (Database) store;
        List<Write> writes = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount() * 3; ++i) {
            String key = TestData.getSimpleString();
            TObject value = TestData.getTObject();
            long record = (Math.abs(TestData.getInt()) % 10) + 1;
            add(key, value, record);
            if(Math.abs(TestData.getInt()) % 3 == 0) {
                writes.add(Write.add(key, value, record));
            }
            if(Math.abs(TestData.getInt()) % 3 == 0) {
                db.sync();
            }
        }
        db.stop();
        db.start();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        Iterator<Write> it = writes.iterator();
        while (it.hasNext()) {
            Write write = it.next();
            executor.execute(() -> {
                Assert.assertTrue(db.verify(write.getKey().toString(),
                        write.getValue().getTObject(),
                        write.getRecord().longValue()));
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Override
    protected void add(String key, TObject value, long record) {
        if(!store.verify(key, value, record)) {
            ((Database) store).accept(Write.add(key, value, record));
        }
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(current);
    }

    @Override
    protected Database getStore() {
        current = TestData.DATA_DIR + File.separator + Time.now();
        return new Database(current);
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        if(store.verify(key, value, record)) {
            ((Database) store).accept(Write.remove(key, value, record));
        }
    }

}
