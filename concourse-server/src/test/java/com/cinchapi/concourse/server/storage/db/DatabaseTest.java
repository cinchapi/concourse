/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.StoreTest;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link com.cinchapi.concourse.server.storage.db.Database}.
 *
 * @author Jeff Nelson
 */
public class DatabaseTest extends StoreTest {

    private String current;

    @Test
    public void testDatabaseRemovesUnbalancedBlocksOnStartup()
            throws Exception {
        Database db = (Database) store;
        db.accept(Write.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong()));
        db.triggerSync();
        db.stop();
        FileSystem.deleteDirectory(current + File.separator + "csb");
        FileSystem.mkdirs(current + File.separator + "csb");
        db = new Database(db.getBackingStore()); // simulate server restart
        db.start();
        Field cpb = db.getClass().getDeclaredField("cpb");
        Field csb = db.getClass().getDeclaredField("csb");
        Field ctb = db.getClass().getDeclaredField("ctb");
        cpb.setAccessible(true);
        csb.setAccessible(true);
        ctb.setAccessible(true);
        Assert.assertEquals(1, ((List<?>) ctb.get(db)).size());
        Assert.assertEquals(1, ((List<?>) csb.get(db)).size());
        Assert.assertEquals(1, ((List<?>) cpb.get(db)).size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAllRecords() {
        Database db = (Database) store;
        db.getAllRecords();
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
    @Ignore
    public void testOnDiskStreamingIterator() {
        Database db = (Database) store;
        int count = TestData.getScaleCount() * 5;
        Set<Revision<PrimaryKey, Text, Value>> expected = Sets
                .newLinkedHashSetWithExpectedSize(count);
        for (int i = 0; i < count; ++i) {
            Write write = Write.add(TestData.getSimpleString(),
                    TestData.getTObject(), i);
            db.accept(write);
            Revision<PrimaryKey, Text, Value> revision = Revision
                    .createPrimaryRevision(write.getRecord(), write.getKey(),
                            write.getValue(), write.getVersion(),
                            write.getType());
            expected.add(revision);
            Variables.register("expected_" + i, revision);
            if(i % 100 == 0) {
                db.triggerSync();
            }
        }
        db.triggerSync();
        Iterator<Revision<PrimaryKey, Text, Value>> it = Database
                .onDiskStreamingIterator(db.getBackingStore());
        Iterator<Revision<PrimaryKey, Text, Value>> it2 = expected.iterator();
        int i = 0;
        while (it.hasNext()) {
            Revision<PrimaryKey, Text, Value> actual = it.next();
            Assert.assertEquals(it2.next(), actual);
            Variables.register("actual_" + i, actual);
            ++i;
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
        db.triggerSync();
        db.stop();
        db = new Database(db.getBackingStore()); // TODO: cannot stop/start same
                                                 // Database instance because
                                                 // state isn't reset...
        db.start();
        PrimaryRecord rec = Reflection.call(db, "getPrimaryRecord",
                PrimaryKey.wrap(record), Text.wrap(a)); // (authorized)
        Assert.assertTrue(rec.isPartial());
        db.select(record);
        rec = Reflection.call(db, "getPrimaryRecord", PrimaryKey.wrap(record),
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
