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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.StoreTest;
import com.cinchapi.concourse.server.storage.db.Database.CacheConfiguration;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
    public void testDatabaseRemovesDuplicateSegmentsOnStartupSanityCheck() {
        Database db = (Database) store;
        int count = 4;
        for (int i = 0; i < count; ++i) {
            db.accept(Write.add(TestData.getString(), TestData.getTObject(),
                    Time.now()));
            db.sync();
        }
        List<Segment> segments = Reflection.get("segments", db);
        Segment seg1 = segments.get(1);
        Segment seg2 = segments.get(2);
        Segment merged = Segment.create();
        seg1.writes().forEach(write -> merged.acquire(write));
        seg2.writes().forEach(write -> merged.acquire(write));
        merged.transfer(Paths.get(current).resolve("segments")
                .resolve(UUID.randomUUID() + ".seg"));
        db.stop();
        db.start();
        int expected = 3;
        segments = Reflection.get("segments", db);
        Assert.assertEquals(expected + 1, segments.size()); // size includes
                                                            // seg0
    }

    @Test
    public void testDatabaseRemovesDuplicateSegmentsOnStartup() {
        Database db = (Database) store;
        int expected = TestData.getScaleCount();
        for (int i = 0; i < expected; ++i) {
            db.accept(Write.add(TestData.getString(), TestData.getTObject(),
                    Time.now()));
            db.sync();
        }
        List<Segment> segments = Reflection.get("segments", db);
        Assert.assertEquals(expected + 1, segments.size()); // size includes
                                                            // seg0
        int a = TestData.getScaleCount() % (segments.size() - 2);
        int b = a + 1;
        Segment merged = Segment.create();
        Segment seg1 = segments.get(a);
        Segment seg2 = segments.get(b);
        seg1.writes().forEach(write -> merged.acquire(write));
        seg2.writes().forEach(write -> merged.acquire(write));
        merged.transfer(Paths.get(current).resolve("segments")
                .resolve(UUID.randomUUID() + ".seg"));
        expected -= 1; // Because #seg1 and #seg2 were merged
        db.stop();
        AtomicInteger duplicates = new AtomicInteger(0);
        FileSystem.ls(Paths.get(current).resolve("segments")).forEach(file -> {
            if(Random.getInt() % 3 == 0
                    && !file.toString().endsWith(merged.id())) {
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
        Assert.assertTrue(segments.stream().map(Segment::id)
                .collect(Collectors.toList()).contains(merged.id()));
        for (Segment segment : segments) {
            int count = 0;
            for (Segment seg : segments) {
                if(seg != segment && seg.intersects(segment)) {
                    ++count;
                }
            }
            if(count > 0) {
                Assert.fail(segment + " has " + count + " duplicates");
            }
        }
        System.out.println("The database discarded " + duplicates.get() + 1
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
    @Ignore
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

    @Test
    public void testRepairDuplicateData() {
        Database db = (Database) store;
        Set<Write> duplicates = Sets.newHashSet();
        for (int i = 0; i < 10; ++i) {
            duplicates.add(
                    Write.add(TestData.getSimpleString(), TestData.getTObject(),
                            TestData.getIdentifier().longValue()));
        }
        for (Write write : duplicates) {
            db.accept(write);
        }
        db.sync();
        Write duplicate = null;
        int count = 0;
        while (count < 5 || duplicate == null) {
            boolean addDuplicates = TestData.getScaleCount() % 3 == 0;
            for (int j = 0; j < TestData.getScaleCount(); ++j) {
                Write write = null;
                if(addDuplicates && TestData.getScaleCount() % 5 == 0) {
                    write = Iterables.get(duplicates,
                            Math.abs(TestData.getInt()) % duplicates.size());
                    duplicate = MoreObjects.firstNonNull(duplicate, write);
                }
                else {
                    while (write == null || duplicates.contains(write)) {
                        write = TestData.getWriteAdd();
                    }
                }
                db.accept(write);
            }
            db.sync();
            ++count;
        }
        Write write = duplicate;
        try {
            db.verify(write.getKey().toString(), write.getValue().getTObject(),
                    write.getRecord().longValue());
            Assert.fail("Expected an unoffset Write exception");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            db.select(write.getRecord().longValue());
            Assert.fail("Expected an unoffset Write exception");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        db.repair();
        db.verify(write.getKey().toString(), write.getValue().getTObject(),
                write.getRecord().longValue());
        db.select(write.getRecord().longValue());
    }

    @Test
    public void testRepairDuplicateDataWithAtomicCommit() {
        Database db = (Database) store;
        Set<Write> duplicates = Sets.newHashSet();
        for (int i = 0; i < 10; ++i) {
            duplicates.add(
                    Write.add(TestData.getSimpleString(), TestData.getTObject(),
                            TestData.getIdentifier().longValue()));
        }
        for (Write write : duplicates) {
            db.accept(write);
        }
        db.sync();
        Write duplicate = null;
        int count = 0;
        List<Write> toVerify = Lists.newArrayList();
        while (count < 5 || duplicate == null) {
            boolean addDuplicates = TestData.getScaleCount() % 3 == 0;
            for (int j = 0; j < TestData.getScaleCount(); ++j) {
                Write write = null;
                if(addDuplicates && TestData.getScaleCount() % 5 == 0) {
                    write = Iterables.get(duplicates,
                            Math.abs(TestData.getInt()) % duplicates.size());
                    duplicate = MoreObjects.firstNonNull(duplicate, write);
                }
                else {
                    while (write == null || duplicates.contains(write)) {
                        write = TestData.getWriteAdd();
                    }
                }
                db.accept(write);
            }
            // Simulate Atomic Operation
            Write w1 = Write.add("foo", Convert.javaToThrift(Time.now()),
                    Time.now());
            Write w2 = Write.add("foo", Convert.javaToThrift(Time.now()),
                    Time.now());
            Write w3 = Write.add("foo", Convert.javaToThrift(Time.now()),
                    Time.now());
            toVerify.add(w1);
            toVerify.add(w2);
            toVerify.add(w3);
            db.accept(w1);
            db.accept(w2.rewrite(w1.getVersion()));
            db.accept(w3.rewrite(w1.getVersion()));
            db.sync();
            ++count;
        }
        Write write = duplicate;
        try {
            db.verify(write.getKey().toString(), write.getValue().getTObject(),
                    write.getRecord().longValue());
            Assert.fail("Expected an unoffset Write exception");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            db.select(write.getRecord().longValue());
            Assert.fail("Expected an unoffset Write exception");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        db.repair();
        db.verify(write.getKey().toString(), write.getValue().getTObject(),
                write.getRecord().longValue());
        db.select(write.getRecord().longValue());
        for (Write w : toVerify) {
            Assert.assertTrue(db.verify(w));
        }
    }

    @Test
    public void testVerifyDataAfterReindexNoStop() {
        Database db = (Database) store;
        List<Write> writes = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount() * 3; ++i) {
            String key = TestData.getSimpleString();
            TObject value = TestData.getTObject();
            long record = (Math.abs(TestData.getInt()) % 10) + 1;
            add(key, value, record);
            writes.add(Write.add(key, value, record));
            if(Math.abs(TestData.getInt()) % 3 == 0) {
                db.sync();
            }
        }
        db.reindex();
        Iterator<Write> it = writes.iterator();
        while (it.hasNext()) {
            Write write = it.next();
            Assert.assertTrue(db.verify(write.getKey().toString(),
                    write.getValue().getTObject(),
                    write.getRecord().longValue()));
        }
    }

    @Test
    public void testVerifyDataAfterReindexStop() {
        Database db = (Database) store;
        List<Write> writes = Lists.newArrayList();
        int count = TestData.getScaleCount() * 3;
        for (int i = 0; i < count; ++i) {
            String key = TestData.getSimpleString();
            TObject value = TestData.getTObject();
            long record = (Math.abs(TestData.getInt()) % 10) + 1;
            add(key, value, record);
            writes.add(Write.add(key, value, record));
            if(Math.abs(TestData.getInt()) % 3 == 0 || i == (count - 1)) {
                db.sync();
            }
        }
        db.stop();
        db.start();
        db.reindex();
        Iterator<Write> it = writes.iterator();
        while (it.hasNext()) {
            Write write = it.next();
            Assert.assertTrue(db.verify(write.getKey().toString(),
                    write.getValue().getTObject(),
                    write.getRecord().longValue()));
        }
    }

    @Test
    public void testCacheEvictionIsSizeBased() {
        Database db = (Database) store;
        String key = "test";
        long record = 1;
        db.accept(Write.add(key, Convert.javaToThrift(-1), record));
        db.sync();
        db.stop();
        AtomicBoolean evicted = new AtomicBoolean(false);
        CacheConfiguration cacheConfig = CacheConfiguration.builder()
                .memoryLimit(1000).memoryCheckFrequencyInSeconds(1)
                .evictionListener(removed -> evicted.set(true)).build();
        db = new Database(Paths.get(db.getBackingStore()), cacheConfig); // ensure
                                                                         // caches
                                                                         // are
        db.start();
        Assert.assertEquals(
                ImmutableMap.of(key, ImmutableSet.of(Convert.javaToThrift(-1))),
                db.select(record));
        int count = 100;
        for (int i = 0; i < count; ++i) {
            db.accept(Write.add(key, Convert.javaToThrift(i), record));
        }
        Threads.sleep(1, TimeUnit.SECONDS);
        db.accept(Write.add(key, Convert.javaToThrift(count), record));
        Assert.assertTrue(evicted.get());
    }

    @Test
    public void testDataCorrectnessAfterCacheEviction() {
        Database db = (Database) store;
        String key = "test";
        long record = 1;
        db.accept(Write.add(key, Convert.javaToThrift(-1), record));
        db.sync();
        db.stop();
        AtomicBoolean evicted = new AtomicBoolean(false);
        CacheConfiguration cacheConfig = CacheConfiguration.builder()
                .memoryLimit(1000).memoryCheckFrequencyInSeconds(1)
                .evictionListener(removed -> evicted.set(true)).build();
        db = new Database(Paths.get(db.getBackingStore()), cacheConfig); // ensure
                                                                         // caches
                                                                         // are
        db.start();
        Assert.assertEquals(
                ImmutableMap.of(key, ImmutableSet.of(Convert.javaToThrift(-1))),
                db.select(record));
        int count = 100;
        for (int i = 0; i < count; ++i) {
            db.accept(Write.add(key, Convert.javaToThrift(i), record));
        }
        Threads.sleep(1, TimeUnit.SECONDS);
        db.accept(Write.add(key, Convert.javaToThrift(count), record));
        for (int i = -1; i <= count; ++i) {
            Assert.assertTrue(db.verify(key, Convert.javaToThrift(i), record));
        }
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
