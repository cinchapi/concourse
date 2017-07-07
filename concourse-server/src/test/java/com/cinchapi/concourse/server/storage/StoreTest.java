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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.TObjectSorter;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

/**
 * Base unit tests for {@link Store} services.
 * 
 * @author Jeff Nelson
 */
@RunWith(Theories.class)
public abstract class StoreTest extends ConcourseBaseTest {

    public final Logger log = LoggerFactory.getLogger(getClass());

    @DataPoints
    public static Operator[] operators = { Operator.EQUALS,
            Operator.GREATER_THAN, Operator.GREATER_THAN_OR_EQUALS,
            Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS,
            Operator.NOT_EQUALS };

    @DataPoints
    public static SearchType[] searchTypes = { SearchType.PREFIX,
            SearchType.INFIX, SearchType.SUFFIX, SearchType.FULL };

    protected Store store;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            cleanup(store);
        }

        @Override
        protected void starting(Description desc) {
            store = getStore();
            store.start();
        }
    };

    // TODO test audit

    @Test
    public void testBrowseKey() {
        Multimap<TObject, Long> data = Variables.register("data",
                TreeMultimap.<TObject, Long> create());
        String key = TestData.getSimpleString();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!data.containsEntry(value, record)) {
                    data.put(value, record);
                    add(key, value, record);
                }
            }
        }
        Assert.assertEquals(data.asMap(), store.browse(key));
    }

    @Test
    public void testBrowseKeyAfterRemove() {
        Multimap<TObject, Long> data = Variables.register("data",
                TreeMultimap.<TObject, Long> create());
        String key = TestData.getSimpleString();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!data.containsEntry(value, record)) {
                    data.put(value, record);
                    add(key, value, record);
                }
            }
        }
        Iterator<Entry<TObject, Long>> it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                it.remove();
                remove(key, entry.getKey(), entry.getValue());
            }
        }
        Assert.assertEquals(data.asMap(), store.browse(key));
    }

    @Test
    public void testBrowseKeyAfterRemoveWithTime() {
        Multimap<TObject, Long> data = Variables.register("data",
                TreeMultimap.<TObject, Long> create());
        String key = TestData.getSimpleString();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!data.containsEntry(value, record)) {
                    data.put(value, record);
                    add(key, value, record);
                }
            }
        }
        Iterator<Entry<TObject, Long>> it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                it.remove();
                remove(key, entry.getKey(), entry.getValue());
            }
        }
        long timestamp = Time.now();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                remove(key, entry.getKey(), entry.getValue());
            }
        }
        Assert.assertEquals(data.asMap(), store.browse(key, timestamp));
    }

    @Test
    public void testGetAllRecords() {
        Set<Long> set = Sets.newTreeSet();
        for (long i = 50; i <= 100; i++) {
            add("name", Convert.javaToThrift("foo" + i), i);
            set.add(i);
        }
        Assert.assertEquals(set, store.getAllRecords());
    }

    @Test
    public void testBrowseKeyAfterRemoveWithTimeReproCON_91() {
        Multimap<TObject, Long> data = Variables.register("data",
                TreeMultimap.<TObject, Long> create());
        data.put(Convert.javaToThrift(-2982699655776463047L),
                -3332967120782416036L);
        data.put(Convert.javaToThrift(-2982699655776463047L),
                -3193024454871429052L);
        data.put(Convert.javaToThrift(723284932), 2021923430868945807L);
        data.put(
                Convert.javaToThrift(
                        "6y1vg56zfge6n u xpfk88zsteez5klmdmde7mux45hope d2ixtgd"),
                -5698094812015896631L);
        data.put(
                Convert.javaToThrift(
                        "6y1vg56zfge6n u xpfk88zsteez5klmdmde7mux45hope d2ixtgd"),
                -1784224494277607728L);
        data.put(
                Convert.javaToThrift(
                        "6y1vg56zfge6n u xpfk88zsteez5klmdmde7mux45hope d2ixtgd"),
                -1661462551451553081L);
        data.put(Convert.javaToThrift("7478v4flnf2hy4uq856q5j1u4yu"),
                -4055175164196245068L);
        data.put(Convert.javaToThrift("7478v4flnf2hy4uq856q5j1u4yu"),
                7242075887519601694L);
        data.put(Convert.javaToThrift(0.18700446070413457),
                -1455745637934964252L);
        data.put(Convert.javaToThrift(0.55897903), -4790445645749745356L);
        data.put(Convert.javaToThrift(1233118838), -3117864874339953135L);
        data.put(Convert.javaToThrift(1233118838), -3117864874339953135L);
        data.put(Convert.javaToThrift(1375924251), -5136738009956048263L);
        data.put(
                Convert.javaToThrift(
                        "kqoc3badp43aryq4kqjy sxp1ywhemli cvtajepz 04oxro0dt3oykn y4pexibpkms0 8uu4ncac2xauc1exc 19ija"),
                -4997919281599660112L);
        Iterator<Entry<TObject, Long>> it = data.entries().iterator();
        String key = "foo";
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            add(key, entry.getKey(), entry.getValue());
        }
        it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                it.remove();
                remove(key, entry.getKey(), entry.getValue());
            }
        }
        long timestamp = Time.now();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<TObject, Long> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                remove(key, entry.getKey(), entry.getValue());
            }
        }
        Assert.assertEquals(data.asMap(), store.browse(key, timestamp));

    }

    @Test
    public void testBrowseKeyIsSorted() {
        String key = TestData.getSimpleString();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        Map<TObject, Set<Long>> data = Variables.register("data",
                store.browse(key));
        TObject previous = null;
        for (TObject current : data.keySet()) {
            if(previous != null) {
                Variables.register("previous", previous);
                Variables.register("current", current);
                Assert.assertTrue(
                        TObjectSorter.INSTANCE.compare(previous, current) < 0);
            }
            previous = current;
        }
    }

    @Test
    public void testBrowseKeyReproCON_90() {
        Multimap<TObject, Long> data = TreeMultimap.create();
        data.put(Convert.javaToThrift(1734274782), 6257334921322559283L);
        data.put(Convert.javaToThrift(-703772218), 3593682749530207485L);
        data.put(Convert.javaToThrift(false), -1767406137984980364L);
        data.put(Convert.javaToThrift(true), 6811881757006335381L);
        data.put(Convert.javaToThrift(true), -5977092512633522530L);
        data.put(Convert.javaToThrift(654569943), -8123653947218958724L);
        data.put(Convert.javaToThrift(654569943), 832046070249085659L);
        data.put(Convert.javaToThrift(717735738), 2456974634159417208L);
        data.put(Convert.javaToThrift(717735738), 3663570106751188709L);
        data.put(
                Convert.javaToThrift(
                        "2ldexok y9mqipnui o4w85kfa55t9nuzk212kvmf mqvm nr u3412xu6df2gx gsk5 lzv4ssghrbs 3ljiea8 8e2mwauu 12"),
                -4614329651952703136L);
        data.put(
                Convert.javaToThrift(
                        "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb"),
                -5807028703649467961L);
        data.put(
                Convert.javaToThrift(
                        "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb"),
                2471646868604570488L);
        data.put(
                Convert.javaToThrift(
                        "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb"),
                6897578410324076954L);
        data.put(
                Convert.javaToThrift(
                        "8vsgpp4i4sqo 7wcqxx6342lpai1lypm8icw6yrpkrbwknf51ho1 y5i9d4x"),
                -6242678134557131181L);
        data.put(
                Convert.javaToThrift(
                        "8y6s9mfwedl21tnk8 ad m  gknrl3 do67lqo1k2yb1soi z  bfhga  k2xu4u rnkui p03ou"),
                -2131365700818384077L);
        data.put(
                Convert.javaToThrift(
                        "9 6g9swglj86ko96vstgq0bcv ml66ekw1 z7rce zi4wfk"),
                3226024846745901977L);
        data.put(Convert.javaToThrift(4324130441596932925L),
                -1255236281809723953L);
        data.put(Convert.javaToThrift(0.41255849314087956),
                4541543718475851175L);
        data.put(Convert.javaToThrift(0.5102137787300446),
                -7644876211582281943L);
        data.put(Convert.javaToThrift(0.5102137787300446),
                2763166646777350749L);
        data.put(Convert.javaToThrift(0.6456127773032042),
                -3312861494325403139L);
        data.put(Convert.javaToThrift(0.7039659687563723),
                -1306475581312320073L);
        data.put(Convert.javaToThrift(0.7039659687563723),
                7844239138927869378L);
        data.put(Convert.javaToThrift(0.7039659687563723),
                7897178695680416538L);
        data.put(
                Convert.javaToThrift(
                        "eixhm9et65tb0re4vfnnrjr8d70840hjhr6koau6vfj2qv76vft"),
                4184600990944636146L);
        data.put(
                Convert.javaToThrift(
                        "k5 qk0abvcjpgj5qdk byot4n9pc8axs4gf4kacb7baolebri vluvkboq"),
                -7758530170278083935L);
        data.put(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr"),
                -8655447648200374519L);
        data.put(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr"),
                -618794245900638337L);
        data.put(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr"),
                193250295130615638L);
        data.put(
                Convert.javaToThrift(
                        "nu xgp dz  aln3vk xrezcsv tikkpdrwod 0rp4byh8 ngv8ppvd4j dxkrnfsn0"),
                -3521699612493033909L);
        data.put(
                Convert.javaToThrift(
                        "nu xgp dz  aln3vk xrezcsv tikkpdrwod 0rp4byh8 ngv8ppvd4j dxkrnfsn0"),
                -3284699155987771538L);
        doTestBrowseKeyRepro(data);
    }

    @Test
    public void testBrowseKeyWithTime() {
        Multimap<TObject, Long> data = Variables.register("data",
                TreeMultimap.<TObject, Long> create());
        String key = TestData.getSimpleString();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!data.containsEntry(value, record)) {
                    data.put(value, record);
                    add(key, value, record);
                }
            }
        }
        long timestamp = Time.now();
        for (TObject value : getValues()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                long record = TestData.getLong();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        Assert.assertEquals(data.asMap(), store.browse(key, timestamp));
    }

    @Test
    public void testBrowseRecord() {
        Multimap<String, TObject> data = Variables.register("data",
                HashMultimap.<String, TObject> create());
        long record = TestData.getLong();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!data.containsEntry(key, value)) {
                    data.put(key, value);
                    add(key, value, record);
                }
            }
        }
        Assert.assertEquals(data.asMap(), store.select(record));
    }

    @Test
    public void testBrowseRecordAfterRemove() {
        Multimap<String, TObject> data = Variables.register("data",
                HashMultimap.<String, TObject> create());
        long record = TestData.getLong();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!data.containsEntry(key, value)) {
                    data.put(key, value);
                    add(key, value, record);
                }
            }
        }
        Iterator<Entry<String, TObject>> it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<String, TObject> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                it.remove();
                remove(entry.getKey(), entry.getValue(), record);
            }
        }
        Assert.assertEquals(data.asMap(), store.select(record));
    }

    @Test
    public void testBrowseRecordAfterRemovesWithTime() {
        Multimap<String, TObject> data = Variables.register("data",
                HashMultimap.<String, TObject> create());
        long record = TestData.getLong();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!data.containsEntry(key, value)) {
                    data.put(key, value);
                    add(key, value, record);
                }
            }
        }
        Iterator<Entry<String, TObject>> it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<String, TObject> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                it.remove();
                remove(entry.getKey(), entry.getValue(), record);
            }
        }
        long timestamp = Time.now();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        it = data.entries().iterator();
        while (it.hasNext()) {
            Entry<String, TObject> entry = it.next();
            if(TestData.getScaleCount() % 3 == 0) {
                remove(entry.getKey(), entry.getValue(), record);
            }
        }
        Assert.assertEquals(data.asMap(), store.select(record, timestamp));

    }

    @Test
    public void testChronologize() {
        Map<Long, Set<TObject>> expected = Maps.newLinkedHashMap();
        Set<TObject> set = Sets.newLinkedHashSet();
        Set<TObject> allValues = Sets.newLinkedHashSet();
        long recordId = TestData.getLong();
        for (long i = 30; i <= 35; i++) {
            TObject tObject = null;
            while (tObject == null || !allValues.add(tObject)) {
                tObject = TestData.getTObject();
                add("name", tObject, recordId);
                set.add(tObject);
            }
        }
        long start = Time.now();
        for (long i = 36; i <= 45; i++) {
            set = Sets.newLinkedHashSet(set);
            TObject tObject = null;
            while (tObject == null || !allValues.add(tObject)) {
                tObject = TestData.getTObject();
                add("name", tObject, recordId);
            }
            set.add(tObject);
            expected.put(i, set);
        }
        for (long i = 46; i <= 50; i++) {
            set = Sets.newLinkedHashSet(set);
            Iterator<TObject> it = allValues.iterator();
            if(it.hasNext()) {
                TObject tObject = it.next();
                if(i % 2 == 0) {
                    remove("name", tObject, recordId);
                    set.remove(tObject);
                }
            }
            expected.put(i, set);
        }
        long end = Time.now();
        for (long i = 51; i <= 55; i++) {
            TObject tObject = null;
            while (tObject == null || !allValues.add(tObject)) {
                tObject = TestData.getTObject();
                add("name", tObject, recordId);
            }
        }
        Map<Long, Set<TObject>> actual = store.chronologize("name", recordId,
                start, end);
        long key = 36;
        for (Entry<Long, Set<TObject>> e : actual.entrySet()) {
            Set<TObject> result = e.getValue();
            Assert.assertEquals(expected.get(key), result);
            key++;
        }
    }

    @Test
    public void testBrowseRecordIsSorted() {
        Multimap<String, TObject> data = Variables.register("data",
                HashMultimap.<String, TObject> create());
        long record = TestData.getLong();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!data.containsEntry(key, value)) {
                    data.put(key, value);
                    add(key, value, record);
                }
            }
        }
        Map<String, Set<TObject>> result = Variables.register("data",
                store.select(record));
        String previous = null;
        for (String current : result.keySet()) {
            if(previous != null) {
                Variables.register("previous", previous);
                Variables.register("current", current);
                Assert.assertTrue(previous.compareToIgnoreCase(current) < 0);
            }
            previous = current;
        }
    }

    @Test
    public void testBrowseRecordWithTime() {
        Multimap<String, TObject> data = Variables.register("data",
                HashMultimap.<String, TObject> create());
        long record = TestData.getLong();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!data.containsEntry(key, value)) {
                    data.put(key, value);
                    add(key, value, record);
                }
            }
        }
        long timestamp = Time.now();
        for (String key : getKeys()) {
            for (int i = 0; i < TestData.getScaleCount() % 4; i++) {
                TObject value = TestData.getTObject();
                if(!store.verify(key, value, record)) {
                    add(key, value, record);
                }
            }
        }
        Assert.assertEquals(data.asMap(), store.select(record, timestamp));
    }

    @Test
    public void testCaseInsensitiveSearchLower() { // CON-10
        String key = Variables.register("key", "foo");
        TObject value = null;
        while (value == null || GlobalState.STOPWORDS.contains(value.toString())
                || GlobalState.STOPWORDS
                        .contains(value.toString().toUpperCase())
                || GlobalState.STOPWORDS
                        .contains(value.toString().toLowerCase())
                || Strings.isNullOrEmpty(
                        TStrings.stripStopWords(value.toString()))) {
            value = Variables.register("value",
                    Convert.javaToThrift(TestData.getString().toUpperCase()));
        }
        long record = (long) Variables.register("record", 1);
        String query = Variables.register("query",
                value.toString().toLowerCase());
        add(key, value, record);
        Assert.assertTrue(store.search(key, query).contains(record));
    }

    @Test
    public void testCaseInsensitiveSearchReproA() {
        String key = Variables.register("key", "foo");
        TObject value = Variables.register("value",
                Convert.javaToThrift("5KPRAN6MT7RR X  P  ZBC4OMD0"));
        long record = (long) Variables.register("record", 1);
        String query = Variables.register("query",
                "5kpran6mt7rr x  p  zbc4omd0");
        add(key, value, record);
        Assert.assertTrue(store.search(key, query).contains(record));
    }

    @Test
    public void testCaseInsensitiveSearchUpper() {
        String key = Variables.register("key", "foo");
        TObject value = null;
        while (value == null || GlobalState.STOPWORDS.contains(value.toString())
                || GlobalState.STOPWORDS
                        .contains(value.toString().toLowerCase())
                || GlobalState.STOPWORDS
                        .contains(value.toString().toUpperCase())
                || Strings.isNullOrEmpty(
                        TStrings.stripStopWords(value.toString()))) {
            value = Variables.register("value",
                    Convert.javaToThrift(TestData.getString().toLowerCase()));
        }
        long record = (long) Variables.register("record", 1);
        String query = Variables.register("query",
                value.toString().toUpperCase());
        add(key, value, record);
        Assert.assertTrue(store.search(key, query).contains(record));
    }

    @Test
    public void testDescribeAfterAddAndRemoveSingle() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.describe(record).contains(key));
    }

    @Test
    public void testDescribeAfterAddAndRemoveSingleWithTime() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.describe(record, timestamp).contains(key));
    }

    @Test
    public void testDescribeAfterAddMulti() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        for (String key : keys) {
            add(key, TestData.getTObject(), record);
        }
        Assert.assertEquals(keys, store.describe(record));
    }

    @Test
    public void testDescribeAfterAddMultiAndRemoveMulti() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        int count = 0;
        for (String key : keys) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Iterator<String> it = keys.iterator();
        count = 0;
        while (it.hasNext()) {
            String key = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, Convert.javaToThrift(count), record);
            }
            count++;
        }
        Assert.assertEquals(keys, store.describe(record));
    }

    @Test
    public void testDescribeAfterAddMultiAndRemoveMultiWithTime() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        int count = 0;
        for (String key : keys) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Iterator<String> it = keys.iterator();
        count = 0;
        while (it.hasNext()) {
            String key = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, Convert.javaToThrift(count), record);
            }
            count++;
        }
        long timestamp = Time.now();
        count = 0;
        for (String key : getKeys()) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Assert.assertEquals(keys, store.describe(record, timestamp));
    }

    @Test
    public void testDescribeAfterAddMultiWithTime() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        for (String key : keys) {
            add(key, TestData.getTObject(), record);
        }
        long timestamp = Time.now();
        for (String key : getKeys()) {
            add(key, TestData.getTObject(), record);
        }
        Assert.assertEquals(keys, store.describe(record, timestamp));
    }

    @Test
    public void testDescribeAfterAddSingle() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        add(key, TestData.getTObject(), record);
        Assert.assertTrue(store.describe(record).contains(key));
    }

    @Test
    public void testDescribeAfterAddSingleWithTime() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, TestData.getTObject(), record);
        Assert.assertFalse(store.describe(record, timestamp).contains(key));
    }

    @Test
    public void testDescribeEmpty() {
        Assert.assertTrue(store.describe(TestData.getLong()).isEmpty());
    }

    @Test
    public void testFetchAfterAddAndRemoveSingle() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.select(key, record).contains(value));
    }

    @Test
    public void testFetchAfterAddAndRemoveSingleWithTime() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.select(key, record, timestamp).contains(value));
    }

    @Test
    public void testFetchAfterAddMulti() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        Assert.assertEquals(values, store.select(key, record));
    }

    @Test
    public void testFetchAfterAddMultiAndRemoveMulti() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        Iterator<TObject> it = values.iterator();
        while (it.hasNext()) {
            TObject value = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, value, record);
            }
        }
        Assert.assertEquals(values, store.select(key, record));
    }

    @Test
    public void testFetchAfterAddMultiAndRemoveMultiWithTime() {
        String key = Variables.register("key", TestData.getSimpleString());
        long record = Variables.register("record", TestData.getLong());
        Set<TObject> values = Variables.register("values", getValues());
        for (TObject value : values) {
            add(key, value, record);
        }
        Iterator<TObject> it = values.iterator();
        while (it.hasNext()) {
            TObject value = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, value, record);
            }
        }
        long timestamp = Time.now();
        List<TObject> otherValues = Variables.register("otherValues",
                Lists.<TObject> newArrayList());
        for (TObject value : getValues()) {
            while (values.contains(value) || otherValues.contains(value)) {
                value = TestData.getTObject();
            }
            add(key, value, record);
            otherValues.add(value);
        }
        Set<TObject> valuesCopy = Sets.newHashSet(values);
        for (TObject value : getValues()) {
            if(valuesCopy.contains(value) || otherValues.contains(value)) {
                remove(key, value, record);
                otherValues.remove(value);
                valuesCopy.remove(value);
            }
        }
        Assert.assertEquals(values, store.select(key, record, timestamp));
    }

    @Test
    public void testFetchAfterAddMultiWithTime() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        long timestamp = Time.now();
        Set<TObject> otherValues = Sets.newHashSet();
        for (TObject value : getValues()) {
            while (values.contains(value) || otherValues.contains(value)) {
                value = TestData.getTObject();
            }
            otherValues.add(value);
            add(key, value, record);
        }
        Assert.assertEquals(values, store.select(key, record, timestamp));
    }

    @Test
    public void testFetchAfterAddSingle() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.select(key, record).contains(value));
    }

    @Test
    public void testFetchAfterAddSingleWithTime() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, value, record);
        Assert.assertFalse(
                store.select(key, record, timestamp).contains(value));
    }

    @Test
    public void testFetchEmpty() {
        Assert.assertTrue(
                store.select(TestData.getSimpleString(), TestData.getLong())
                        .isEmpty());
    }

    @Test
    public void testFindLinksTo() {
        String key = Variables.register("key", TestData.getSimpleString());
        long source = Variables.register("source", TestData.getLong());
        long destination = Variables.register("destination",
                TestData.getLong());
        while (source == destination) {
            destination = Variables.register("destination", TestData.getLong());
        }
        add(key, Convert.javaToThrift(Link.to(destination)), source);
        Set<Long> results = Variables.register("results", store.find(key,
                Operator.LINKS_TO, Convert.javaToThrift(destination)));
        Assert.assertTrue(results.contains(source));
    }

    @Test
    @Theory
    public void testFind(Operator operator) {
        String key = TestData.getSimpleString();
        Number min = TestData.getNumber();
        Set<Long> records = addRecords(key, min, operator);
        Assert.assertEquals(records,
                store.find(key, operator, Convert.javaToThrift(min)));
    }

    @Test
    public void testFindIntLongEquality() {
        // Related to http://jira.cinchapi.com/browse/CON-326
        add("foo", Convert.javaToThrift(1L), 1);
        Assert.assertEquals(Sets.newHashSet(1L),
                store.find("foo", Operator.EQUALS, Convert.javaToThrift(1)));
    }

    @Test
    public void testFindForRegexWithPercentSign() {
        String key = Variables.register("key", TestData.getSimpleString());
        String value = Variables.register("value", TestData.getString());
        Set<Long> records = Variables.register("records", getRecords());
        for (long record : records) {
            add(key, Convert.javaToThrift(value), record);
        }
        Assert.assertEquals(records, store.find(key, Operator.REGEX,
                Convert.javaToThrift(putStringWithinPercentSign(value))));
    }

    @Test
    public void testFindForNotRegExWithPercentSign() {
        String key = Variables.register("key", TestData.getSimpleString());
        String value1 = Variables.register("value1", TestData.getString());
        Set<Long> records1 = Variables.register("records1", getRecords());
        for (long record : records1) {
            add(key, Convert.javaToThrift(value1), record);
        }
        String value2 = null;
        while (value2 == null || value2.contains(value1)) {
            value2 = Variables.register("value2", TestData.getString());
        }
        Set<Long> records2 = Variables.register("records2", getRecords());
        for (long record : records2) {
            add(key, Convert.javaToThrift(value2), record);
        }
        Assert.assertEquals(records2, store.find(key, Operator.NOT_REGEX,
                Convert.javaToThrift(putStringWithinPercentSign(value1))));
    }

    @Test
    public void testFindForNotRegexWithPercentSignReproA() {
        String key = Variables.register("key",
                "pklbwoj8p1fwni89ra339ytdzu11m6ttmaynni5zxzwi402gpfoui2fba0w6r3580esv8pv3xphy8ohffod2g");
        String value1 = Variables.register("value1", "a");
        Set<Long> records1 = Variables.register("records1",
                Sets.newHashSet(-8837327677807046246L, -1837928815572945895L,
                        -7042182654721884696L, 3142018574192978144L,
                        -6639179481432426018L, 461806750568583298L,
                        -5449875477758503155L, 1395727263052630755L,
                        4363963785781396592L, -8485487848254456506L,
                        -7931250504437226728L, 6017151736071373350L,
                        -2893502697295133660L, 2052546698363219491L,
                        2410155758617125738L, 2849478253048385138L,
                        6586957270677760116L, -1822986183439476271L,
                        -4186548993362340144L, -727399974550900574L,
                        3688062601296251410L));
        for (long record : records1) {
            add(key, Convert.javaToThrift(value1), record);
        }
        String value2 = Variables.register("value2",
                "l5gewgae55y59xyyj63w8x6f5mphssiyh327x5k5q1x z4sbr0xh5il6");
        while (value2 == null || value2.contains(value1)) {
            value2 = Variables.register("value2", TestData.getString());
        }
        Set<Long> records2 = Variables.register("records2",
                Sets.newHashSet(-6182791895483854312L, -679172883778660965L,
                        1120463509328983993L, -8479770926286484152L,
                        1128382420337449323L, 6257301028647171984L,
                        6823367565918477224L, 2330855273859656550L,
                        7177177908301439818L, -8094395763130835882L,
                        5898816101052626932L, -4557467144755416551L,
                        -2755758238783715284L, 2886417267455105816L,
                        1943598759101180077L, 263040801152290323L,
                        7552043432119880007L, -7277413805920665985L,
                        -4117831401170893413L, 7400570047490749104L,
                        6722954364072475529L));
        for (long record : records2) {
            add(key, Convert.javaToThrift(value2), record);
        }
        Assert.assertEquals(records2, store.find(key, Operator.NOT_REGEX,
                Convert.javaToThrift(putStringWithinPercentSign(value1))));
    }

    @Test
    @Theory
    public void testFindAfterRemove(Operator operator) {
        String key = TestData.getSimpleString();
        Number min = TestData.getNumber();
        Set<Long> records = removeRecords(key, addRecords(key, min, operator));
        Assert.assertEquals(records,
                store.find(key, operator, Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testFindAfterRemoveWithTime(Operator operator) {
        String key = TestData.getSimpleString();
        Number min = TestData.getNumber();
        Set<Long> records = removeRecords(key, addRecords(key, min, operator));
        long timestamp = Time.now();
        removeRecords(key, addRecords(key, min, operator));
        Assert.assertEquals(records, store.find(timestamp, key, operator,
                Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testFindWithTime(Operator operator) {
        String key = TestData.getSimpleString();
        Number min = TestData.getNumber();
        Set<Long> records = addRecords(key, min, operator);
        long timestamp = Time.now();
        addRecords(key, min, operator);
        Assert.assertEquals(records, store.find(timestamp, key, operator,
                Convert.javaToThrift(min)));
    }

    @Test
    public void testFindThatRecordWithValueAsTagIsIncludedInResultSet() {
        String key = TestData.getSimpleString();
        Tag value = Tag.create(TestData.getString());
        Set<Long> records = addRecords(key, value, Operator.NOT_EQUALS);
        Long tagRecord = null;
        while (tagRecord == null || records.contains(tagRecord)) {
            tagRecord = TestData.getLong();
        }
        Set<Long> resultRecords = Sets.newHashSet();
        resultRecords.add(tagRecord);
        add(key, Convert.javaToThrift(value), tagRecord);
        Assert.assertEquals(resultRecords,
                store.find(key, Operator.EQUALS, Convert.javaToThrift(value)));
    }

    @Test
    public void testFindThatRecordWithValueAsTagAndEqualStringValueInAnotherRecordIsIncludedInResultSet() {
        String key = TestData.getSimpleString();
        String value = TestData.getString();
        Set<Long> records = getRecords();
        for (long record : records) {
            add(key, Convert.javaToThrift(value), record);
        }
        Set<Long> newRecords = Sets.newHashSet();
        Long newRecord = null;
        while (newRecord == null
                || newRecords.size() < TestData.getScaleCount()) {
            newRecord = TestData.getLong();
            if(!records.contains(newRecord)) {
                newRecords.add(newRecord);
                records.add(newRecord);
            }
        }
        for (long record : newRecords) {
            add(key, Convert.javaToThrift(Tag.create(value)), record);
        }
        Assert.assertEquals(records,
                store.find(key, Operator.EQUALS, Convert.javaToThrift(value)));
    }

    @Test
    public void testCantAddDuplicateTagOrStringValueToTheSameKeyInRecord() {
        String key = TestData.getSimpleString();
        long record = TestData.getLong();
        String value = "string1";
        add(key, Convert.javaToThrift(value), record);
        add(key, Convert.javaToThrift(Tag.create(value)), record);
        value = "string2";
        add(key, Convert.javaToThrift(Tag.create(value)), record);
        add(key, Convert.javaToThrift(value), record);
        value = "string3";
        add(key, Convert.javaToThrift(Tag.create(value)), record);
        add(key, Convert.javaToThrift(Tag.create(value)), record);
        Variables.register("test", store.select(key, record));
        Assert.assertEquals(3, store.select(key, record).size());
    }

    @Test
    @Theory
    public void testSearch(SearchType type) {
        String query = null;
        while (query == null) {
            query = TestData.getString();
        }
        Variables.register("query", query);
        String key = Variables.register("key", TestData.getSimpleString());
        Set<Long> records = setupSearchTest(key, query, type);
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    public void testSearchIsLimitedToKey() {
        add("foo", Convert.javaToThrift("importer"), 1);
        add("bar", Convert.javaToThrift("importer"), 2);
        Assert.assertTrue(store.search("foo", "importer").contains(1L));
        Assert.assertFalse(store.search("foo", "importer").contains(2L));
    }

    @Test
    @Theory
    public void testSearchReproA(SearchType type) {
        String query = Variables.register("query", "tc e");
        String key = Variables.register("key", "26f0wzcw2ixadcfsa");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(6631928579149921621L), Lists.newArrayList(
                        "qi2sqa06xhn5quxbdasrtjsrzucbmo24fc4u78iv4rtc1ea7dnas74uxadvrf"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproB(SearchType type) {
        String query = Variables.register("query",
                "6w07u z a3euaaekb13li7je0b2jyeaztu5se9xsi");
        String key = Variables.register("key",
                "2vuagrm1hkhrnjt2nf1n411ch7djphag6bgrxw9fcpe6c7zqfvny7z6n");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1L), Lists.newArrayList(
                        "6w07u z a3euaaekb13li7je0b2jyeaztu5se9xsi"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproC(SearchType type) {
        String query = Variables.register("query",
                "i7rwvli7esvitzio2 qp  arxwlclruja ulzfhtl4yyxopsc  bk57q2tz30 0y606dwynvffp6vqx");
        String key = Variables.register("key", "foo");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1L), Lists.newArrayList(
                        "1ub0gsi61bz39y90wbe96rvxo3g4mtt89sg1tfjsf4vuyyjc9oivc7sluuxrmj897ni15p8obu6i7rwvli7esvitzio2 qp  arxwlclruja ulzfhtl4yyxopsc  bk57q2tz30 0y606dwynvffp6vqx"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproCON_18(SearchType type) {
        String query = Variables.register("query", "w 3");
        String key = Variables.register("key", "woq80jx4j1ij");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(-8637216143534516416L, 421096814721703284L,
                        6564122985024885236L, -5952107620777355272L,
                        -436640785454846021L, -7907647268823148126L,
                        -7992244480937113688L, 6064517325574202294L,
                        -1886417763916136006L, 4854806921088381487L,
                        3311512385033793458L, -1757909211467275410L,
                        -7084408514542196387L, 7275080592617062380L,
                        -7426898229188673692L, 754480952648110863L,
                        424433091429876284L, 5165325020869171306L,
                        5886947254936812579L, 1115795719781445848L,
                        7389722247458313374L),
                Lists.newArrayList("3wi92e09gvonunyyw00vnfwx2i7dgiwcs6v0pr",
                        "kb3ys9il8gnl1wc6m4rb0cs2z8j89kjtjqu8wkf5jbsrl27kyke1x4ltm163ll74",
                        "km ne995y83xlkqzc u  uhb h 951 876qx7n3xjsg lqt0hbk4 1g2u1q0z59bm1dww 4 9rlvjbff",
                        "w 3qslmhu ttqh08ldb ctp5z44r2dhry1dsmje2rztap28itjzg2bpbcqwf0b2lx43xw8kl vki0vj0jouh9 s1x5yse",
                        "4sx wy325g4kle5scr3 gp1zo26nubpz488", "f188snr",
                        "f y8nvz5lpycrze 679ek7qwi1p8pc7 bxzidbcu0nb ztjc0xqt7uxlumrqam7k0h",
                        "vzr3cnv1 78kv0631o9f5929zonowngtklz2dzslsrvhch8mtu74x6fzubqv8w6r90dciz1os4gcuchjrq",
                        "6e7vo amq bro6pbk06sryi2v3i05ictx0ptu55 1o11os57fmhl3vlmcpt3x1a7uo0w w6",
                        "w 322v2ecbhntjmcr 14lbicqw5mvl5oe0 uphj26tgfpu",
                        "w 36uwvdkfeuetxlvg96hqf ivuy85 nng0hl3 bpx rne61585979se52x2 k9w9hiw",
                        "4crjb7 t8cqam7",
                        "w 3ycfkh3fhb6jlte1e03wobxl2ppb9g5y8jx0muzbnr9trvw86k24q1dhyo7ajg5q75e",
                        "2mfbjku04id4g zeafhdi3oxz4325",
                        "w 3jepc5omist7y9gbeljfir9cu9hr yws04ktrrtbbetv0lpidc tvjdnfxzbpugfd0 0au714z7bltou",
                        "o0d06p8g7t ic7o 14tzu84cn6riz8 9n3 paez1ne9uu8r03g7y5p91ao8rdh b5pojbxvqxn",
                        "9xujari8qzz 6kdyer4pwe6l5y t6dv7x9p95 220 lq3nzjtxp70ugvt1mp2d82oq99na 56iynv3si",
                        "r45fyzsgs 30u03aje0g 30p6gl677a1",
                        "jpl7 3a4x66g215khx471tk644 e73ihlc 12 849wmjbwx1qg0b4wzjc5jx 12bwf1m20pzjme 9fsazu7h8wql umyx66",
                        "usy3ee7opw6mwm80y0sgs1 eegoodgk4skemp 2repeg5y0lmzwano38s83",
                        "qhlyg2pnnztz zloczyixrf434mjikutox2v62ukfsjh9a"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproCON_2(SearchType type) {
        String query = Variables.register("query", "k i");
        String key = Variables.register("key", "oq99f7u7vizpob4o");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1902752458578581194L, 1959599694661426747L,
                        -9154557670941699129L, -984115036014491508L,
                        5194905945498812204L, 5521526792899195281L,
                        4893428588236612746L, -6469751959947965328L,
                        -6053272917723840881L, -3780108145350335994L,
                        3428649801035140268L, 8581751009047755290L,
                        -8274546879060748693L, 4433133031784539226L,
                        2539259213139177697L),
                Lists.newArrayList(
                        "mbqg6 ls47i09bqt76lhtefjevbxt v9gc mqhily6agpweptt7dj jl a vbvqisuz4d xky18xs",
                        "9j6eue5m2 rn rovj4p4y bh kderm7xw40epfmbibtay6l2 x6 cw 833uhgkde43bwy8 b5u5rrlzdmqx",
                        "z22eb7likgl1xd1 bna93h2y  2nq  1kfejkmk iz22eb7likgl1xd1 bna93h2y  2nq  1kfejkm",
                        "n3a6dsk7gdhhp5re0piodkkvb085q7b7jj7bac0m27t6hhhajwyf",
                        "i4jfmy3nnfiupbnf04ecthucbj4pzisu4xpqy78k ii4jfmy3nnfiupbnf04ecthucbj4pzisu4xpqy78",
                        "b8yljef75 lvfwevcbb sg40mtoaovr2g8lgpgkcu88kprfdms7qncflm8wx0e9a9zt0zx8uvy4yf0mnqg",
                        "qfih uzg8 7 cy euxg 7sz8i8mj c40czvac6yk b worw65  3wkwhtc etulr1b9gsww puk iqfih uzg8 7 cy euxg 7sz8i8mj c40czvac6yk b worw65  3wkwhtc etulr1b9gsww pu",
                        "yte1xocdz agzid h3juda8fwpehyztqcc9ka2jb5",
                        "j1nl2lvd5ie",
                        "zqw e tfvd9y 4i7921apde59kfetaxcqcj89 s 1c5ncb t",
                        "simk a7 s7oh1 oz9wfrh7830q82hoorvfomcw8dzy9eaku cvu1pdknxwkcf1w9",
                        "eurti8wfy244clx15u", "ig5 bq",
                        "y9rf7s 14y8o c8kraxfd714e9r9rqzq  ghoctaln2g 24dxirf ewwskvu5p7pn1h80s1nn fd88 z1c8k5dx7z0i5xhk iy9rf7s 14y8o c8kraxfd714e9r9rqzq  ghoctaln2g 24dxirf ewwskvu5p7pn1h80s1nn fd88 z1c8k5dx7z0i5xh",
                        "s93z3eggrxiuyb1enl59y  gwu7gn2cj 1luh j  pj"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproCON_7(SearchType type) {
        String query = Variables.register("query", "5");
        String key = Variables.register("key", "vhncr15x4vi1r7dx3bw8sgo3");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(4407787408868251656L, -81405110977674943L,
                        -7140839054266835785L, -2478927665696010310L,
                        218763128369680085L, 3303203363767514564L,
                        -5345452149459798319L, -2878094606020740280L,
                        -2364262306786179717L, -7073499293604063929L,
                        -1181274448039222311L, 74405258499052471L,
                        -3199762627894970848L, -1589943203396786380L,
                        3917764129096184859L, 5340961473029303427L,
                        -6765165614843560497L, -7193164394167202080L,
                        -2953063992651183477L, 2015523299602625665L,
                        8352547665716061424L),
                Lists.newArrayList("x6ovyg2524ez1n",
                        "x7 e0o5ouxxbs8 ykcj248ss873ds94x71eli1 7mvo963e9wnhc k36dek6t0hde8h6tc2bji436jislz2o 497nvdzn0ugr e",
                        "yn6isk627adyvm0j7k1l jm2xffyyciluvhrzu xrpn6607o pfyq1 3biam5b7odvnt4",
                        "0m0du5135vcnlmvv 924 e7ao0enajqri",
                        "kdqrlcg9b857na3 pf9cb4n4gqkuk1gza7z8gst",
                        "g yk9juwviyrts8pb6pwmu9inwue27y86ugqe5u2lloo2 o3t7bknpzb7705tdkjso9v5d5mz42l0z81zga2wjbvdb21ld2o9gey",
                        "qz7v 974khsb24bfukqehsfqpkgosifd   4t3 xoas0fp",
                        "xng2awwtylp5l6jsza1hyqg90zinagt p 2bzbawvx0 k0phvb5q o3fd",
                        "jyqy8cybzu7jxrhqxskxwsho8db9pan5a1gzuejssdluy5ja748nk0n0ii7ceq13n3ytd5",
                        "skvu5 i0a9i0mj3xrx7hnkjydlhh70uuautvw5qmkmhlzgda vtj42hr0 c02ahksg3xip6fo9n7 eaa3yo9gu",
                        "im2cseewi75t6ky45xcetc0hx6vpbr3p3vosgkre0j1pbtw8t550dbq71qt5",
                        "ofcak2so66bsljj1ne7c fmu2rns4aqcq7tk987yus3s7",
                        "mand ww0pn6b2q8q3qpzajuay0  ma   w34s59",
                        "hxjdt37fs73h7vpx2xods2lcd55ok4vc1egqbww7eup8qfajchjoalfjau4syzd6m8s5utqpcidyfw",
                        "ieai43wrv1 ss 09r0s 1b14oij70uu9 j2z9zu16bta cwk8p41jb y nmjxlws9",
                        "o6s 2ys3 kr9x6xqu5rpa5xjmmca19n khznppiqbam9wz bn4u6ole9wy7yxd869uyd79p7t11wepn3 uxh78kz7wbvm",
                        "h5 lkwji9axrh875rl",
                        "hbuyv6y6ma241d37tpb5btlxh542a0s2w9eq8riubmiv2s64vshhlbv0yv1ecdopaw7x5ymvyixp8ayakh6r10yi7t03",
                        "0vme9t s4xl44x  c oni rw8g1m g6oc2o3jdcm0i4u1y4x6 b i4gtk 7 glerao 3rty s5c54uddn2t95",
                        "0ek185xzbdlxbj8ci012nglwcbr19u7vvwfexec s0autjoj2ot99n2zt0g44y5y3hgz5",
                        "tf4rry2ru8wlf03vi8c1yi5vk8vaz19tqguukjey 6xs6as epal78hl4stg1t8634mc4v o7885"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproD(SearchType type) {
        String query = Variables.register("query", "0 tihr2 nva7zd z96x");
        String key = Variables.register("key", "foo");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1L), Lists.newArrayList(
                        "xqwqd3q522  3hnlzuu22qctkmc5 4xwxdz52iesr6nd820ihe5j6iz5 rn8g 0vkwzl  jxjyb494lhvhmqu9cvzuc3v9wnngx0 tihr2 nva7zd z96xxqwqd3q522  3hnlzuu22qctkmc5 4xwxdz52iesr6nd820ihe5j6iz5 rn8g 0vkwzl  jxjyb494lhvhmqu9cvzuc3v9wnngx"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproE(SearchType type) {
        String query = Variables.register("query",
                "5 6ib73dp0b dwjjfa8pcfgd8uz0y0k t6eueqd4cjgujg2d7j825e8f  lxt7khroy30 ");
        String key = Variables.register("key", "foo");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1L), Lists.newArrayList(
                        "5 6ib73dp0b dwjjfa8pcfgd8uz0y0k t6eueqd4cjgujg2d7j825e8f  lxt7khroy30"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproF(SearchType type) {
        String query = Variables.register("query", "34 y");
        String key = Variables.register("key",
                "gpvokxzt84dsbm2ylhsooalv0fyhqukc");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(-4473008077619333882L, -8351207459167435413L,
                        -4459427393681524497L, 4939907340215037652L,
                        -6886124796950548141L, 5712289740182081898L,
                        -5352871050404061986L, 9078216912764450349L,
                        368752644783891597L, -731408778453015380L,
                        2833136306920397583L, 8970944616841919904L,
                        -2557996967788292936L, 3011361709098214638L,
                        9131206744594670786L, -269685979046038300L,
                        -7557829318518279649L, -2403971717188586598L,
                        7144064178377245110L, -1901968551799565728L,
                        4040139761808173392L, -6949001582626254753L,
                        -3136222895901082164L, 5566791123710361043L,
                        5050276518482596641L, -8202827082220536933L,
                        8450772276451962700L, 2171722260496954622L,
                        8691364971781746515L, 1772440781478734988L,
                        -3401640223866128972L, -1892754300252444576L,
                        -9049673414695221207L, -2759568085991866192L,
                        2220378323437157183L),
                Lists.newArrayList("es5wpos73i60li2sv17trbgas5j7",
                        "p9xbp2jf0q134 yp9xbp2jf0q1",
                        "5xkc8h642h9i9y15pb b13q4mlentzvnslco2 e7dzxaxyodxgwr0ktqghr8sbgnptppk5ztnakenk0b9nh hohf",
                        "249isiqqwpkeeyzluyo8k87fb7z2tu36ybvkpek9jyoflwxpjsgow80tqupnzi0kks4ch1zzfdyz7nd6wpwj3djxt81eh6v0jk4v34 y249isiqqwpkeeyzluyo8k87fb7z2tu36ybvkpek9jyoflwxpjsgow80tqupnzi0kks4ch1zzfdyz7nd6wpwj3djxt81eh6v0jk4v",
                        "zgakm3nmk5n24ibpbhdigskr 1 3se5p2kmhaparqmk50vmwgwgisip6otvbnckkb7dv",
                        "qwnu9d6xj 1zzxd2rg snnevznghg4 1nehuqpdid06vjt8",
                        "ilnp05hlx34 yilnp05hlx",
                        "y yz8k j558gk47ir0l02nccnxovsb8mnyt rus1ps98iqveavt7fxeil",
                        "mop i",
                        "7ctzo7n4zdt0udlw2r122qdbnfiu7kxj4bopbs7u6tsrxahp01gqnkl20ezic2z7fu kxe vr58n",
                        "bzzsys ish7rs5 35a5jnfc76 pc u76pp9w60p7kqsm",
                        "qgqmt4mkalkllogybpiwp54aw151435k01taohihs30ydmc4",
                        "35m8tdk58 iaa1d1d",
                        "c6hfmlp1yh6eqxuhln3jcr1ix4mjfo1sclz1k3xsj75jebx79wejpyje34 yc6hfmlp1yh6eqxuhln3jcr1ix4mjfo1sclz1k3xsj75jebx79wejpyje",
                        "kmno8f3bl9m4mvkhq 3hknzwwplzv7oybwhxkrde 5hc74qcq0  srswuav9cl",
                        "5garx88 4kaq4zg8z0yps3o1ipymfftcqp a0icy0fpfcgcd9ys67f7yql6vv5xhmh41vmo834 y5garx88 4kaq4zg8z0yps3o1ipymfftcqp a0icy0fpfcgcd9ys67f7yql6vv5xhmh41vmo8",
                        "3z4y8o9yynujfip4ia zgnhw5p2qzlmk5ngygexo70o262bu2j4wn3itwwy ph35s11zf5 4mv8kkfxse",
                        "zkmsu22sun6qtozwyjyqe c137cstb98ex 8lja3sg",
                        "6h1v3ar4z45p 574wohd9lp6y qo8vuwp91lrj03bq4wp7mgzt2qjdv jst3nmi7huvgga6cn",
                        "xoe7xrkxezla4yqx4ckulogibruuggnw1bta34iobrme0mq8fb",
                        "f8vd4ehlrc8",
                        "tgr0rtl98pod047tbxfc6pi52y9onh9jzdt4cj6zb8ofb5gt7c0o87f59qlwjps1u1rih2tb",
                        "v76eyuorcqk6czzk3 s5p1f95o81ywv1i48izht0v7yx1k7ebewgomjwgcjixe1v1n2bu5g7zkjkuj9h5kmd521m9",
                        "stk4bjrb08n143le9ij4534 ystk4bjrb08n143le9ij45",
                        "15j0dy4uf3ychzayss7die",
                        "0 ua72nxf nosl8msw34 y0 ua72nxf nosl8msw",
                        "x253x2bbcjxxtuegqk68q d2ku lz3k0dvmj8hf2zbmh25b",
                        "xt8v7sir9uqt12f9tcq12gp2k27e3k12oe4wbpf3ob9t3pqjpg6zci4d 5y534 yxt8v7sir9uqt12f9tcq12gp2k27e3k12oe4wbpf3ob9t3pqjpg6zci4d 5y5",
                        "4yz10yezarmf34 y4yz10yezarmf", "o8r1m34 yo8r1m",
                        "lqe462we526s7tnc39ia8e 2dhq7iojy5wwx4uj4i08hk9b6kx074ppjpj1 it2rk4rh9vxe7zrgvv9ibyj0t2h7wm",
                        "ndpehrrvwa32cxzc7uouly1ys39vg8 khpmv5kqwnwazdzchfmxk1p6vp4hj4pvcy8q019w89ktjc2p0p2n957th37 7o834 yndpehrrvwa32cxzc7uouly1ys39vg8 khpmv5kqwnwazdzchfmxk1p6vp4hj4pvcy8q019w89ktjc2p0p2n957th37",
                        "u2 kgkkdf9gshpfnkj8539m059k8e749erbwb1075men1xn9g1xyfl5grb4xfkjwji5bbxblmt",
                        "vp4lxzi4sgbeybp3t  sdbiv2oonm02i06phf0s b7f4sixhq 1w 2cqdklb ic ue96z9f",
                        "radc6q2a6 qlxi2wmqfwykevv dx2o ij5o 07u9ba4ukygfha7 t 8 pavj1tgm4 nl9p laz7enfg0sq35 3k5233 "));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproG(SearchType type) {
        String query = Variables.register("query", "w  8");
        String key = Variables.register("key", "kqlqkg");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(-277580307881000612L, -5255148899331404410L,
                        -6420115819351478982L, 6866110824999006259L,
                        -4492041108193153731L, 3352888707677394801L,
                        -2860842597796167717L, -6888793119513119872L,
                        8726162261843572788L, -1009173904547593201L,
                        -2751278487748655682L, -2345450785095954225L,
                        -6366403077042069753L, 6192140380630656305L,
                        2742476713515734831L, 5255788341749360850L,
                        -1599496265723822053L, -8972410565681248229L,
                        8390960620509055056L),
                Lists.newArrayList("zntcnz 2ob317e2hkhui2dneo69rkej0a5qj",
                        "opsrx2y5fqkxclc3ux bf0pxik4r1pddesxsr25yek70z fcojyyojnz9mjfw4r62unjh",
                        "35ckl663czoibu2gxwy9mdsspr9jko694x6qqrkx6kgb9lrpvshq7g9bjzbivp5ajuiyh90y0lkuckl2qmvhdmw  835ckl663czoibu2gxwy9mdsspr9jko694x6qqrkx6kgb9lrpvshq7g9bjzbivp5ajuiyh90y0lkuckl2qmvhdm",
                        "33sd4z7 0gys0py9ipbjltz jp3lvbqw  833sd4z7 0gys0py9ipbjltz jp3lvbq",
                        "q4p13puyp7nxroteiak30jwaew  8q4p13puyp7nxroteiak30jwae",
                        "uo0qgmr6r66mfuligawh08f33ce63uubwuaue186r6x0g9bwwqg9c4wooctgu72a5kksbepajevzkfpjny2osj6pu0ryk3o",
                        "snou", "6mwg3pl6f1hy2qq6agh",
                        "wsnv12h c5xa1rk099 rumx9jdhf9pf e1",
                        "t2j62i78z39r2hyt49q9 lsdoeq7tuyczcqyh4ar lpx1gubry4bb 26tymj6bkrktcq10lq8xj65yqv8bt8b3flxb 2zoqp",
                        "7ra3 67i9t3q8azuq9j fu5tt28mg 0j3qvposw  87ra3 67i9t3q8azuq9j fu5tt28mg 0j3qvpos",
                        "i2f d0g",
                        "onc1l75d7xyk6jm4rebv89jy05jurpk651h8xygc5eol6b6ufpl4s9hifc3etv7d6iv84fzb70ua1363605pf4owb8w  8onc1l75d7xyk6jm4rebv89jy05jurpk651h8xygc5eol6b6ufpl4s9hifc3etv7d6iv84fzb70ua1363605pf4owb8 ",
                        "zs94", "hbhq3c9ll li1jz9cplbbzrt t9vt14r",
                        "cdb24id6us  cn k49egrbbqqysww0wzgdndm0kkmer05so8mvnt99499xqygldqoh3aqpf xscrlsnuugnw  8cdb24id6us  cn k49egrbbqqysww0wzgdndm0kkmer05so8mvnt99499xqygldqoh3aqpf xscrlsnuugn",
                        "ww1bevsi9j9u07fpw",
                        "i6qzle7myjtb p8zzvr48jle jb951bhnjrbz1 r455s9 g9zbgth9ugexl  fl55d8h4a08r wnd2q",
                        "9iltboo1ejsy08cmd9562lwni9"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproH(SearchType type) {
        String query = Variables.register("query", "4  w");
        String key = Variables.register("key",
                "viho04sfbnz8zdfqsdyw9zwk5o2genjl2engu28ap0uyzxgyv9wc");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(1378249483279686062L, -6161014232753001252L,
                        8433543546648615058L, 417782169298412285L,
                        -3539644780337239226L, -714055345939181564L,
                        -4659589834251389440L, 2974961501049931858L,
                        1498065777882054524L, 1714252475302608813L,
                        -5234135322684760593L, -7612584946477396261L,
                        9023646044357780211L, -2252683871509119265L,
                        -8081867832259689703L, -374594252064464588L,
                        8025017904883834843L, -7866768561866704644L),
                Lists.newArrayList("eg lb36kp t4",
                        "lt rzrttfbhglt0423181mgtk le5", "qc8vkeyk294mr8",
                        "7bbfk6lsinjfy0n4c1hr0gwe2zso8yalti4oefzzstv274q6twl90f033mb4vlg3ch19  afnodtnm1p4  w",
                        "2fc1p1ksst88ut70u38weagn4q5ziwfqad4o4ldpu5z9shdcpr21p6t4quebt7ehzptphxw3j8x5f",
                        "al75cmqjmzv9uy6difn95gu11pq5csb3", "cquxx8kt",
                        "h919c0 ct79xk4h98x", "dq70ohga8ze79m9hygwjtjtbn2g",
                        "omr8n rvjc pcg9k1uc8yv0ut7n w3f ouwg0 91 r5hr7h7p419ancsm",
                        "56uotxg3daytxa zpi6e6 w 83dst7 ujenhtr1muzxg8n cnt t3h4yytwuvjf0k5hc2gcvk9vh 92zcr6p fdvu2qg",
                        "thsleyyy628wzeciiv", "i94  w",
                        "1zw2jj2r510sjg0sqauagjzpk5yt9jgcc2iiu5dy6i85kwi511esjihep9zk3p11nde",
                        "1z0ef75 poaz6h1v1903f9 xkvmq fpf1o3mb4v4xd2o1n u azamd8oanwmz46c163ta77c2rlc4ad6 9qhnqegqpk9os",
                        "uk7c1pmx tp9ytkk2p35ekyogtiwgblgf3d1b5bl aw5bbnh7odic9h",
                        "ut3x5hs0 sivxixboqn36p107if0g1v4u54  w",
                        "g989rxtkel4 g3vc85 0b42iektum6y610pxtuml"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproI(SearchType type) {
        String query = Variables.register("query",
                "qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we");
        String key = Variables.register("key",
                "vorrrsfuqdwatipwhgpjmihbapuynizs");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(-2431843905487157607L), Lists.newArrayList(
                        "b6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qrqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4web6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qr"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproII(SearchType type) {
        String query = Variables.register("query",
                "qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we");
        String key = Variables.register("key",
                "vorrrsfuqdwatipwhgpjmihbapuynizs");
        Set<Long> records = setupSearchTest(key, query, type,
                Lists.newArrayList(-2431843905487157607L, 2535161598937199860L,
                        5095913505665814359L, 2945479239457230071L,
                        -2513720049182776314L, 8923575282282264235L,
                        7280418409255262422L, -8000254698736898498L,
                        -3043508904014702429L, 2211406443873218662L,
                        -7348339637703367869L, 5435248830548669108L,
                        -2755421732547710398L, 2993100915391110764L,
                        -8799666964100514438L, -2812740815887775575L,
                        -4872192819525015386L, 3125028925810661014L,
                        -4965934597574110522L, -880884727200872586L,
                        -8817932140072154864L, -7911836183919921720L,
                        -6035976570265185529L, 7241269312752994462L,
                        -8020518575166416431L, -8635845075000684890L,
                        -4503895988433291010L, 6607557585769321334L,
                        7192849599491803852L),
                Lists.newArrayList("l6mxivaxi8vd2ygz2puool6isbbzf5vzy1o84",
                        "8lgc17ianf4tf8htbbe31ocq5mj daq58 bf7ofguwb1 ftojmyo4hg2ak t3gzc 6o8ztk8 gqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we8lgc17ianf4tf8htbbe31ocq5mj daq58 bf7ofguwb1 ftojmyo4hg2ak t3gzc 6o8ztk8 g",
                        "ar05gk",
                        "f2pvjmn1671mjkmm4uloa6bskrgv04u2qesrn58zz6kqfhlbeh z zwkd61wva1g7 kw123tid9t njoli4wgyqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4wef2pvjmn1671mjkmm4uloa6bskrgv04u2qesrn58zz6kqfhlbeh z zwkd61wva1g7 kw123tid9t njoli4wgy",
                        "wih5pqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4wewih5p",
                        "5 een  4c0bfnu ft 4p ou i2",
                        "0wvkkqo1dvma1pcm9u81wif5cavirn3t1nz461prsv5869s872yzkoolthetxgw",
                        "uijp4avq9bsz7e4iucyd6c003jt7z6ucb02qb1n2ukk7naq0hinyj0uzj1fkjnixl026tylacqjct93x4f7cnv",
                        "q38194m8nzsdqiefxfae169k70bybfwlqxsatmazzbhmw174x7l5s6rxe2vibc32ochis6 qxwvuq70ni9qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4weq38194m8nzsdqiefxfae169k70bybfwlqxsatmazzbhmw174x7l5s6rxe2vibc32ochis6 qxwvuq70ni9",
                        "eryrkgn90z",
                        "qh2x0sb121jenw4rruncnui2bshr718cg7ewu8n4x42ntijublo4m2bja14z27u9u8stvbkaia8",
                        "jzc18xpy3gdrwqmp0ctxan96dhrs3vuru1xbkhr2vexp38vvety44ei57wrwkk3td4gkqrspthvke844w2tyl4u0yb3w3hnkjcqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4wejzc18xpy3gdrwqmp0ctxan96dhrs3vuru1xbkhr2vexp38vvety44ei57wrwkk3td4gkqrspthvke844w2tyl4u0yb3w3hnkjc",
                        "59qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we59",
                        "r96o2gtoxp2x5dm42jdwbbs7 6sr685jyh6ktnrw9m0 33sn63jr9yb7jkc kcz83z2jywbr008owu ii6bila2koxmd",
                        "gmd th8mznhgnhn0hnd80a 5 l5jbbq0bhysvfipac3jqv1l0spn 72qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4wegmd th8mznhgnhn0hnd80a 5 l5jbbq0bhysvfipac3jqv1l0spn 72",
                        "b6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qrqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4web6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qr",
                        "59ntdmgilu7f1om3kui578rmsrj7ci14wccf",
                        "1 ydx99e77 sgp4qnhh9ni9nutaridtyel9yle506 yg6mcci3nqt4ytodxin0y7gnqag7  pmgit0kc8qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we1 ydx99e77 sgp4qnhh9ni9nutaridtyel9yle506 yg6mcci3nqt4ytodxin0y7gnqag7  pmgit0kc8",
                        "4qzxx350 m17 7i 5xyu1udf417z8aibid6j a67 9t1h7vfbc1j8z3inlt7pqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we4qzxx350 m17 7i 5xyu1udf417z8aibid6j a67 9t1h7vfbc1j8z3inlt7p",
                        "9cv9 a5upqth9lrbmfj  pw79e f t2q6xa9j67z5935iflxxsgoy xtf71iu1b2d k1bp",
                        "pt y 7ss 010ypyi8ilfah x ytqgbgm79qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4wept y 7ss 010ypyi8ilfah x ytqgbgm79",
                        "6dxrhr bzkh2 efurlfpxk w10frpmy95lvap25074 sodfmd4r396kl  5bc 4ocf53ed67vm95vd3ncfs g86ozfn",
                        "ql9cqzlvy",
                        "cig9ggwc0mxr917r4hoauewn76s4258pr22fyuxrqxj183hq0hlainl29ypgudgblreqr19qatw7j",
                        "k9qta8mh537xwt rnzn3mq01fmswyao7o7 late1vf n4axp69s67d55ur21sxz1mzhdq6tvd8jlbg 6gi0",
                        "6gvih9 utkj dn9n3d1 odjy93nb laqzcva8requ7wjfnq w2sn5uwoqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we6gvih9 utkj dn9n3d1 odjy93nb laqzcva8requ7wjfnq w2sn5uwo",
                        "8lw6zxnue4qs3 3nr5csdiw0ilk4hwwvzkcfmcm15no aw34 k4a6yrrwyitkqp",
                        "q1ckhpya2nfr a8l4ho44ucsweulcn8ex1q3653la2yno8e7ob8jpb39bpeqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4weq1ckhpya2nfr a8l4ho44ucsweulcn8ex1q3653la2yno8e7ob8jpb39bpe",
                        "fhyuhdnns c ya7ovi8vrrdg9  kojlv92481b5 zaeyv4um16jnl0bkf46yo"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Ignore("not implemented in Limbo")
    public void testSearchResultSorting() {
        // FIXME this is not implemented in Limbo (cause its very difficult) so
        // right now search result order is undefined).
        String key = Variables.register("key", TestData.getSimpleString());
        String query = Variables.register("query", TestData.getString());
        Map<Long, String> words = Variables.register("words",
                Maps.<Long, String> newTreeMap());
        for (long i = 0; i < 10; i++) {
            String word = null;
            while (Strings.isNullOrEmpty(word)
                    || TStrings.isInfixSearchMatch(query, word)) {
                word = TestData.getString();
            }
            for (long j = 0; j <= i; j++) {
                word += " " + query;
                String other = null;
                while (Strings.isNullOrEmpty(other)
                        || TStrings.isInfixSearchMatch(query, other)) {
                    other = TestData.getString();
                }
                word += " " + other;
            }
            words.put(i, word);
            add(key, Convert.javaToThrift(word), i);
        }
        Set<Long> expected = Variables.register("expected",
                Sets.newTreeSet(Collections.<Long> reverseOrder()));
        expected.addAll(words.keySet());
        Set<Long> actual = Variables.register("actual",
                store.search(key, query));
        Iterator<Long> it = expected.iterator();
        Iterator<Long> it2 = actual.iterator();
        while (it.hasNext()) {
            Assert.assertEquals(it.next(), it2.next());
        }
    }

    @Test
    public void testSearchThatRecordWithValueAsTagIsNotIncludedInResultSet() {
        String key = Variables.register("key", TestData.getSimpleString());
        Tag value = Variables.register("value",
                Tag.create(TestData.getString()));
        Set<Long> records = addRecords(key, value, Operator.NOT_EQUALS);
        Long tagRecord = null;
        while (tagRecord == null || records.contains(tagRecord)) {
            tagRecord = Variables.register("tagRecord", TestData.getLong());
        }
        add(key, Convert.javaToThrift(value), tagRecord);
        Assert.assertFalse(
                store.search(key, value.toString()).contains(tagRecord));
    }

    @Test
    public void testSearchThatRecordWithValueAsTagIsNotIncludedInResultSetReproCON_129() {
        String key = "yy2mf7yveeprn5u1znljubdmld8r2w";
        Tag value = Tag.create("1");
        Long tagRecord = -2641333647249146582L;
        add(key, Convert.javaToThrift(
                "btq0adgux53hjckphjeux7x1sxemyfpsdzipvy032n7t9daxkmw1h7r7zyl60ks5t06zjdjuj4iooq"),
                285009080280006567L);
        add(key, Convert.javaToThrift("7pu1v97xoz5063p9cuq2qoks"),
                -7352212869558049531L);
        add(key, Convert.javaToThrift(false), 388620935878197713L);
        add(key, Convert.javaToThrift(
                "2m5lwamprzq4msvvs2wnv08zcqzi4newhl745qodce22h9yy812"),
                1548639509905032340L);
        add(key, Convert.javaToThrift("e ysho"), -765676142204325002L);
        add(key, Convert
                .javaToThrift("jzfttlm258jejhsuapeqybe2j8fej3t7fgb2t6lqbbj"),
                2679248400003802470L);
        add(key, Convert.javaToThrift("s4i0ite7fep"), -2412570382637653495L);
        add(key, Convert.javaToThrift(
                "6o42czhg72u4u9w2gqfvrnc6c7tm3kp1811u6oi04ri8it5pomhxqx3h71omavvk5pmu4hgl10v00549e"),
                -1087503013401908104L);
        add(key, Convert.javaToThrift("ob4yhyvk076c0 ock"),
                -9186255645112595336L);
        add(key, Convert.javaToThrift(
                "4n8c8bfiyjv0q6niyd6wa2l2s01s2g9jkq9y2dqbkz08zjcrmnbtf5vnyzflwthqcfxp o"),
                8074263650552255137L);
        add(key, Convert.javaToThrift(false), -1122802924122720425L);
        add(key, Convert.javaToThrift(0.6491074), 8257322177342234041L);
        add(key, Convert.javaToThrift(false), 2670863628024031952L);
        add(key, Convert.javaToThrift(0.7184745217075929),
                6804414020539721485L);
        add(key, Convert.javaToThrift(value), tagRecord);
        Set<Long> searchResult = store.search(key, value.toString());
        Variables.register("searchResult", searchResult);
        Assert.assertFalse(searchResult.isEmpty()); // "1" does get indexed
                                                    // from other values
    }

    @Test
    public void testSearchSubstringThatRecordWithValueAsTagIsNotIncludedInResultSet() {
        String key = TestData.getSimpleString();
        String value = null;
        Long tagRecord = TestData.getLong();
        while (value == null || value.length() == 0) {
            value = TestData.getString();
        }
        add(key, Convert.javaToThrift(Tag.create(value)), tagRecord);
        Integer startIndex = null;
        Integer endIndex = null;
        while (startIndex == null || endIndex == null
                || startIndex >= endIndex) {
            startIndex = Math.abs(TestData.getInt()) % value.length();
            endIndex = Math.abs(TestData.getInt()) % value.length() + 1;
        }
        Assert.assertFalse(
                store.search(key, value.substring(startIndex, endIndex))
                        .contains(tagRecord));
    }

    @Test
    @Theory
    @Ignore("CON-8")
    public void testSearchWithStopWordSubStringInQuery() {
        add("string", Convert.javaToThrift("but foobar barfoo"), 1);
        Assert.assertTrue(
                store.search("string", "ut foobar barfoo").contains(1));
    }

    @Test
    public void testVerifyAfterAdd() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.verify(key, value, record));
    }

    @Test
    public void testVerifyAfterAddAndRemove() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.verify(key, value, record));
    }

    @Test
    public void testVerifyAfterAddAndRemoveWithTime() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.verify(key, value, record, timestamp));
    }

    @Test
    public void testVerifyAfterAddWithTime() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, value, record);
        Assert.assertFalse(store.verify(key, value, record, timestamp));
    }

    @Test
    public void testVerifyEmpty() {
        Assert.assertFalse(store.verify(TestData.getSimpleString(),
                TestData.getTObject(), TestData.getLong()));
    }

    @Test
    public void testContains() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertFalse(store.contains(record));
        add(key, value, record);
        Assert.assertTrue(store.contains(record));
        remove(key, value, record);
        Assert.assertTrue(store.contains(record));
    }

    @Test
    public void testReproCON_516() {
        add("name", Convert.javaToThrift("Jeff"), 1);
        Assert.assertFalse(store
                .find("name", Operator.EQUALS, Convert.javaToThrift("jeff"))
                .isEmpty());
    }

    @Test
    public void testLongDoesNotEqualLink() {
        add("friend", Convert.javaToThrift(1), 2);
        Assert.assertTrue(
                store.find("friend", Operator.LINKS_TO, Convert.javaToThrift(1))
                        .isEmpty());
    }

    /**
     * Add {@code key} as {@code value} to {@code record} in the {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void add(String key, TObject value, long record);

    /**
     * Cleanup the store and release and resources, etc.
     * 
     * @param store
     */
    protected abstract void cleanup(Store store);

    /**
     * Return a Store for testing.
     * 
     * @return the Store
     */
    protected abstract Store getStore();

    /**
     * Remove {@code key} as {@code value} from {@code record} in {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void remove(String key, TObject value, long record);

    /**
     * Add {@code key} as a value that satisfies {@code operator} relative to
     * {@code min}.
     * 
     * @param key
     * @param min
     * @param operator
     * @return the records added
     */
    private Set<Long> addRecords(String key, Object min, Operator operator) {
        Set<Long> records = getRecords();
        int count = 0;
        for (long record : records) {
            Object n = null;
            while (n == null
                    || (operator == Operator.GREATER_THAN
                            && Objects.isLessThanOrEqualTo(n, min))
                    || (operator == Operator.GREATER_THAN_OR_EQUALS
                            && Objects.isLessThan(n, min))
                    || (operator == Operator.LESS_THAN
                            && Objects.isGreaterThanOrEqualTo(n, min))
                    || (operator == Operator.LESS_THAN_OR_EQUALS
                            && Objects.isGreaterThan(n, min))
                    || (operator == Operator.NOT_EQUALS
                            && Objects.isEqualTo(n, min))
                    || (operator == Operator.EQUALS
                            && !Objects.isEqualTo(n, min))) {
                n = operator == Operator.EQUALS ? min : TestData.getObject();
            }
            TObject value = Convert.javaToThrift(n);
            add(key, value, record);
            Variables.register(operator + "_write_" + count,
                    key + " AS " + value + " IN " + record);
            count++;
        }
        return records;
    }

    /**
     * Do the repro work for the {@link #testBrowseKey} repros.
     * 
     * @param data
     */
    private void doTestBrowseKeyRepro(Multimap<TObject, Long> data) {
        String key = "foo";
        Variables.register("data", data);
        for (Entry<TObject, Long> entry : data.entries()) {
            add(key, entry.getKey(), entry.getValue());
        }
        Assert.assertEquals(data.asMap(), store.browse(key));
    }

    /**
     * Return a set of keys.
     * 
     * @return the keys.
     */
    private Set<String> getKeys() {
        Set<String> keys = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            String key = null;
            while (key == null || keys.contains(key)) {
                key = TestData.getSimpleString();
            }
            keys.add(key);
        }
        return keys;
    }

    /**
     * Return a set of primary keys.
     * 
     * @return the records
     */
    private Set<Long> getRecords() {
        Set<Long> records = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            Long record = null;
            while (record == null || records.contains(record)) {
                record = TestData.getLong();
            }
            records.add(record);
        }
        return records;
    }

    /**
     * Return a set of TObject values
     * 
     * @return the values
     */
    private Set<TObject> getValues() {
        Set<TObject> values = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            TObject value = null;
            while (value == null || values.contains(value)) {
                value = TestData.getTObject();
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Remove {@code key} from a random sample of {@code records}.
     * 
     * @param key
     * @param records
     * @return the records that remain after the function
     */
    private Set<Long> removeRecords(String key, Set<Long> records) {
        Iterator<Long> it = records.iterator();
        while (it.hasNext()) {
            long record = it.next();
            if(TestData.getInt() % 3 == 0) {
                TObject value = store.select(key, record).iterator().next();
                it.remove();
                remove(key, value, record);
            }
        }
        return records;
    }

    /**
     * Setup a search test by adding some random matches for {@code query} that
     * obey search {@code type} for {@code key} in a random set of records.
     * 
     * @param key
     * @param query
     * @param type
     * @return the records where the query matches
     */
    private Set<Long> setupSearchTest(String key, String query,
            SearchType type) {
        return setupSearchTest(key, query, type, null, null);
    }

    /**
     * Setup a search test by adding some matches for {@code query} that
     * obey search {@code type} for {@code key} in some of the records from
     * {@code recordSource}.
     * 
     * @param key
     * @param query
     * @param type
     * @param recordSource
     * @param otherSource
     * @return the records where the query matches
     */
    private Set<Long> setupSearchTest(String key, String query, SearchType type,
            @Nullable Collection<Long> recordSource,
            @Nullable List<String> otherSource) {
        Preconditions.checkState(recordSource == null
                || (recordSource != null && otherSource != null
                        && recordSource.size() == otherSource.size()));
        Set<Long> records = Sets.newHashSet();
        recordSource = recordSource == null ? getRecords() : recordSource;
        if(!Strings.isNullOrEmpty(TStrings.stripStopWords(query))) {
            int i = 0;
            for (long record : recordSource) {
                if(otherSource != null) {
                    String other = otherSource.get(i);
                    boolean matches = TStrings.isInfixSearchMatch(query, other);
                    SearchTestItem sti = Variables.register("sti_" + record,
                            new SearchTestItem(key, Convert.javaToThrift(other),
                                    record, query, matches));
                    add(sti.key, sti.value, sti.record);
                    if(matches) {
                        records.add(sti.record);
                    }
                }
                else {
                    String other = null;
                    while (other == null || other.equals(query)
                            || TStrings.isInfixSearchMatch(query, other)
                            || TStrings.isInfixSearchMatch(other, query)
                            || Strings.isNullOrEmpty(
                                    TStrings.stripStopWords(other))) {
                        other = TestData.getString();
                    }
                    boolean match = TestData.getInt() % 3 == 0;
                    if(match && type == SearchType.PREFIX) {
                        SearchTestItem sti = Variables.register("sti_" + record,
                                new SearchTestItem(key,
                                        Convert.javaToThrift(query + other),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.SUFFIX) {
                        SearchTestItem sti = Variables.register("sti_" + record,
                                new SearchTestItem(key,
                                        Convert.javaToThrift(other + query),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.INFIX) {
                        SearchTestItem sti = Variables.register("sti_" + record,
                                new SearchTestItem(key,
                                        Convert.javaToThrift(
                                                other + query + other),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.FULL) {
                        SearchTestItem sti = Variables.register("sti_" + record,
                                new SearchTestItem(key,
                                        Convert.javaToThrift(query), record,
                                        query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else {
                        SearchTestItem sti = Variables.register("sti_" + record,
                                new SearchTestItem(key,
                                        Convert.javaToThrift(other), record,
                                        query, false));
                        add(sti.key, sti.value, sti.record);
                    }
                }
                i++;
            }
            return records;
        }
        return records;

    }

    /**
     * This class contains utility methods that provide comparison
     * for the various types including Number, boolean, and String.
     * 
     * @author knd
     */
    private static class Objects {

        /**
         * Compare {@code a} to {@code b}.
         * 
         * @param a
         * @param b
         * @return -1, 0, or 1 as {@code a} is less than, equal to, or
         *         greater than {@code b}.
         */
        private static int compare(Object a, Object b) {
            return Value.Sorter.INSTANCE.compare(
                    Value.wrap(Convert.javaToThrift(a)),
                    Value.wrap(Convert.javaToThrift(b)));
        }

        /**
         * Return {@code true} if {@code a} is equal to {@code b}.
         * 
         * @param a
         * @param b
         * @return {@code true} if {@code a} == {@code b}
         */
        public static boolean isEqualTo(Object a, Object b) {
            Number compareResult = compare(a, b);
            return Numbers.isEqualTo(compareResult, 0);
        }

        /**
         * Return {@code true} if {@code a} is greater than {@code b}
         * 
         * @param a
         * @param b
         * @return {@code true} if {@code a} > {@code b}
         */
        public static boolean isGreaterThan(Object a, Object b) {
            Number compareResult = compare(a, b);
            return Numbers.isGreaterThan(compareResult, 0);
        }

        /**
         * Return {@code true} if {@code a} is greater than or equal
         * to {@code b}.
         * 
         * @param a
         * @param b
         * @return {@code true} if {@code a} >= {@code b}
         */
        public static boolean isGreaterThanOrEqualTo(Object a, Object b) {
            return isGreaterThan(a, b) || isEqualTo(a, b);
        }

        /**
         * Return {@code true} if {@code a} is less than {@code b}.
         * 
         * @param a
         * @param b
         * @return {@code true} if {@code a} < {@code b}
         */
        public static boolean isLessThan(Object a, Object b) {
            Number compareResult = compare(a, b);
            return Numbers.isLessThan(compareResult, 0);
        }

        /**
         * Return {@code true} if {@code a} is less than or equal to {@code b}.
         * 
         * @param a
         * @param b
         * @return {@code true} if {@code a} <= {@code b}
         */
        public static boolean isLessThanOrEqualTo(Object a, Object b) {
            return isLessThan(a, b) || isEqualTo(a, b);
        }
    }

    /**
     * An item that is used in a search test
     * 
     * @author Jeff Nelson
     */
    private class SearchTestItem {

        public final String query;
        public final String key;
        public final TObject value;
        public final long record;
        public final boolean match;

        /**
         * Construct a new instance
         * 
         * @param key
         * @param value
         * @param record
         * @param query
         * @param match
         */
        public SearchTestItem(String key, TObject value, long record,
                String query, boolean match) {
            this.key = key;
            this.value = value;
            this.record = record;
            this.query = query;
            this.match = match;
        }

        @Override
        public String toString() {
            return key + " AS " + value + " IN " + record
                    + (match ? " DOES" : " DOES NOT") + " MATCH " + query;
        }
    }

    /**
     * List of search types
     * 
     * @author Jeff Nelson
     */
    private enum SearchType {
        PREFIX, INFIX, SUFFIX, FULL
    }

    /**
     * This method will put (percent) % sign at both end of the {@link String}.
     * 
     * @param str
     * @return {@code String}
     */
    private String putStringWithinPercentSign(String str) {
        return "%" + str + "%";
    }

}
