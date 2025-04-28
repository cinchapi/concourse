/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.ops;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.data.sort.SortableColumn;
import com.cinchapi.concourse.data.sort.SortableTable;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link Operations}
 *
 * @author Jeff Nelson
 */
public class OperationsTest {

    protected void setupGraph(AtomicSupport store) {
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(2)), 1));
        store.accept(Write.add("name", Convert.javaToThrift("A"), 1));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(3)), 1));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(4)), 1));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(3)), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(1)), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(4)), 2));
        store.accept(Write.add("name", Convert.javaToThrift("B"), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(2)), 3));
        store.accept(Write.add("name", Convert.javaToThrift("C"), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(1)), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(2)), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(5)), 3));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(1)), 4));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(2)), 4));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(3)), 4));
        store.accept(Write.add("name", Convert.javaToThrift("D"), 4));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(5)), 4));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(3)), 4));
        store.accept(Write.add("name", Convert.javaToThrift("E"), 5));
    }

    protected void setupDisconnectedGraph(AtomicSupport store) {
        // Record 1
        store.accept(Write.add("name", Convert.javaToThrift("Alice"), 1));
        store.accept(Write.add("age", Convert.javaToThrift(30), 1));
        store.accept(Write.add("city", Convert.javaToThrift("New York"), 1));
        store.accept(Write.add("score", Convert.javaToThrift(85), 1));
        store.accept(Write.add("active", Convert.javaToThrift(true), 1));

        // Record 2
        store.accept(Write.add("name", Convert.javaToThrift("Bob"), 2));
        store.accept(Write.add("age", Convert.javaToThrift(25), 2));
        store.accept(Write.add("city", Convert.javaToThrift("Boston"), 2));
        store.accept(Write.add("score", Convert.javaToThrift(92), 2));
        store.accept(Write.add("active", Convert.javaToThrift(true), 2));

        // Record 3
        store.accept(Write.add("name", Convert.javaToThrift("Charlie"), 3));
        store.accept(Write.add("age", Convert.javaToThrift(40), 3));
        store.accept(Write.add("city", Convert.javaToThrift("Chicago"), 3));
        store.accept(Write.add("score", Convert.javaToThrift(78), 3));
        store.accept(Write.add("active", Convert.javaToThrift(false), 3));

        // Record 4
        store.accept(Write.add("name", Convert.javaToThrift("Diana"), 4));
        store.accept(Write.add("age", Convert.javaToThrift(35), 4));
        store.accept(Write.add("city", Convert.javaToThrift("Denver"), 4));
        store.accept(Write.add("score", Convert.javaToThrift(88), 4));
        store.accept(Write.add("active", Convert.javaToThrift(true), 4));

        // Record 5
        store.accept(Write.add("name", Convert.javaToThrift("Eve"), 5));
        store.accept(Write.add("age", Convert.javaToThrift(28), 5));
        store.accept(Write.add("city", Convert.javaToThrift("Seattle"), 5));
        store.accept(Write.add("score", Convert.javaToThrift(95), 5));
        store.accept(Write.add("active", Convert.javaToThrift(false), 5));

        // Record 6
        store.accept(Write.add("name", Convert.javaToThrift("Frank"), 6));
        store.accept(Write.add("age", Convert.javaToThrift(45), 6));
        store.accept(Write.add("city", Convert.javaToThrift("Miami"), 6));
        store.accept(Write.add("score", Convert.javaToThrift(81), 6));
        store.accept(Write.add("active", Convert.javaToThrift(true), 6));

        // Record 7
        store.accept(Write.add("name", Convert.javaToThrift("Grace"), 7));
        store.accept(Write.add("age", Convert.javaToThrift(32), 7));
        store.accept(Write.add("city", Convert.javaToThrift("Austin"), 7));
        store.accept(Write.add("score", Convert.javaToThrift(90), 7));
        store.accept(Write.add("active", Convert.javaToThrift(true), 7));

        // Record 8
        store.accept(Write.add("name", Convert.javaToThrift("Henry"), 8));
        store.accept(Write.add("age", Convert.javaToThrift(38), 8));
        store.accept(Write.add("city", Convert.javaToThrift("Portland"), 8));
        store.accept(Write.add("score", Convert.javaToThrift(75), 8));
        store.accept(Write.add("active", Convert.javaToThrift(false), 8));
    }

    @Test
    public void testTraverseKeyRecordAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            String key = "bar.baz.name";
            Set<TObject> data = Operations.traverseKeyRecordOptionalAtomic(key,
                    3, Time.NONE, store);
            Assert.assertTrue(data.isEmpty());
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testBrowseNavigationKeyAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            String key = "foo.bar.baz.name";
            Map<TObject, Set<Long>> data = Operations
                    .browseNavigationKeyOptionalAtomic(key, Time.NONE, store);
            Assert.assertEquals(ImmutableMap.of(Convert.javaToThrift("C"),
                    ImmutableSet.of(1L, 4L), Convert.javaToThrift("D"),
                    ImmutableSet.of(1L, 4L), Convert.javaToThrift("E"),
                    ImmutableSet.of(1L, 4L)), data);
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testTraceRecordAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            AtomicOperation atomic = store.startAtomicOperation();
            Map<String, Set<Long>> incoming = Operations.traceRecordAtomic(2,
                    Time.NONE, atomic);
            Assert.assertEquals(
                    ImmutableMap.of("foo", ImmutableSet.of(1L, 4L), "bar",
                            ImmutableSet.of(3L), "baz", ImmutableSet.of(3L)),
                    incoming);
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testTraceRecordsAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            AtomicOperation atomic = store.startAtomicOperation();
            Map<Long, Map<String, Set<Long>>> incoming = Operations
                    .traceRecordsAtomic(ImmutableList.of(1L, 2L, 3L), Time.NONE,
                            atomic);
            Assert.assertEquals(ImmutableMap.of(2L,
                    ImmutableMap.of("foo", ImmutableSet.of(1L, 4L), "bar",
                            ImmutableSet.of(3L), "baz", ImmutableSet.of(3L)),
                    1L,
                    ImmutableMap.of("bar", ImmutableSet.of(2L), "baz",
                            ImmutableSet.of(3L), "foo", ImmutableSet.of(4L)),
                    3L,
                    ImmutableMap.of("baz", ImmutableSet.of(1L, 4L), "foo",
                            ImmutableSet.of(2L), "bar", ImmutableSet.of(4L))),
                    incoming);
        }
        finally {
            store.stop();
        }
    }

    @Test(expected = InsufficientAtomicityException.class)
    public void testAtomicityIsEnforcedWhenNoTimestamp() {
        AtomicSupport store = getStore();
        setupGraph(store);
        Operations.countKeyAtomic("foo", Time.NONE, store);
    }

    @Test
    public void testAtomicityIsNotEnforcedWithTimestamp() {
        AtomicSupport store = getStore();
        setupGraph(store);
        Operations.countKeyAtomic("foo", Time.now(), store);
        Assert.assertTrue(true);
    }

    @Test
    public void testSelectSingleKeyInOrder() {
        // Test selecting a single key that is also used for ordering.
        // This should use the sort-first approach since the order key is the
        // same as the selection key.
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a page that limits to first 3 results
            Page page = Page.sized(3);

            // Select "name" key and order by "name"
            String key = "name";
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);
            long timestamp = Time.NONE;
            Order order = Order.by("name");
            Supplier<SortableColumn<Set<TObject>>> supplier = () -> SortableColumn
                    .multiValued(key, new LinkedHashMap<>(records.size()));

            Map<Long, Set<TObject>> data = Operations
                    .selectKeyRecordsOptionalAtomic(store, key, records,
                            timestamp, order, page, supplier);

            // Verify correct records are returned in correct order
            Assert.assertEquals(3, data.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(data.keySet());
            Assert.assertEquals(1L, (long) recordOrder.get(0)); // Alice
            Assert.assertEquals(2L, (long) recordOrder.get(1)); // Bob
            Assert.assertEquals(3L, (long) recordOrder.get(2)); // Charlie

            // Verify correct data is returned
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Alice")),
                    data.get(1L));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Bob")),
                    data.get(2L));
            Assert.assertEquals(
                    ImmutableSet.of(Convert.javaToThrift("Charlie")),
                    data.get(3L));
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testSelectSingleKeyNotInOrder() {
        // Test selecting a single key that is different from the ordering key.
        // This should use the sort-first approach since we're paginating and
        // it's more efficient.
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a page that limits to first 3 results
            Page page = Page.sized(3);

            // Select "age" key but order by "name"
            String key = "age";
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);
            long timestamp = Time.NONE;
            Order order = Order.by("name");
            Supplier<SortableColumn<Set<TObject>>> supplier = () -> SortableColumn
                    .multiValued(key, new LinkedHashMap<>(records.size()));

            Map<Long, Set<TObject>> data = Operations
                    .selectKeyRecordsOptionalAtomic(store, key, records,
                            timestamp, order, page, supplier);

            // Verify correct records are returned in correct order
            Assert.assertEquals(3, data.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(data.keySet());
            Assert.assertEquals(1L, (long) recordOrder.get(0)); // Alice
            Assert.assertEquals(2L, (long) recordOrder.get(1)); // Bob
            Assert.assertEquals(3L, (long) recordOrder.get(2)); // Charlie

            // Verify correct data is returned
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(30)),
                    data.get(1L));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(25)),
                    data.get(2L));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(40)),
                    data.get(3L));
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testSelectManyKeysSortFirst() {
        // Test selecting many keys with a small page size.
        // This should use the sort-first approach since the page size is small
        // compared to the total number of records.
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a small page that limits to first 2 results
            Page page = Page.sized(2);

            // Select multiple keys and order by "score" (which is not in the
            // selected keys)
            List<String> keys = ImmutableList.of("name", "city", "active");
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);
            long timestamp = Time.NONE;
            Order order = Order.by("score").descending();
            Supplier<SortableTable<Set<TObject>>> supplier = () -> SortableTable
                    .multiValued(new LinkedHashMap<>());

            Map<Long, Map<String, Set<TObject>>> data = Operations
                    .selectKeysRecordsOptionalAtomic(store, keys, records,
                            timestamp, order, page, supplier);

            // Verify correct records are returned in correct order
            Assert.assertEquals(2, data.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(data.keySet());
            Assert.assertEquals(5L, (long) recordOrder.get(0)); // Eve (score
                                                                // 95)
            Assert.assertEquals(2L, (long) recordOrder.get(1)); // Bob (score
                                                                // 92)

            // Verify correct data is returned for first record
            Map<String, Set<TObject>> record5 = data.get(5L);
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Eve")),
                    record5.get("name"));
            Assert.assertEquals(
                    ImmutableSet.of(Convert.javaToThrift("Seattle")),
                    record5.get("city"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(false)),
                    record5.get("active"));

            // Verify correct data is returned for second record
            Map<String, Set<TObject>> record2 = data.get(2L);
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Bob")),
                    record2.get("name"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Boston")),
                    record2.get("city"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(true)),
                    record2.get("active"));
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testSelectManyKeysSelectFirst() {
        // Test selecting many keys with a large page size.
        // This should use the select-first approach since the page size is
        // large
        // compared to the total number of records.
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a large page that would include all results
            Page page = Page.sized(100);

            // Select multiple keys and order by "age"
            List<String> keys = ImmutableList.of("name", "city", "score",
                    "active");
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);
            long timestamp = Time.NONE;
            Order order = Order.by("age").then("name");
            Supplier<SortableTable<Set<TObject>>> supplier = () -> SortableTable
                    .multiValued(new LinkedHashMap<>());

            Map<Long, Map<String, Set<TObject>>> data = Operations
                    .selectKeysRecordsOptionalAtomic(store, keys, records,
                            timestamp, order, page, supplier);

            // Verify all records are returned in correct order
            Assert.assertEquals(8, data.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(data.keySet());
            Assert.assertEquals(2L, (long) recordOrder.get(0)); // Bob (age 25)
            Assert.assertEquals(5L, (long) recordOrder.get(1)); // Eve (age 28)
            Assert.assertEquals(1L, (long) recordOrder.get(2)); // Alice (age
                                                                // 30)
            Assert.assertEquals(7L, (long) recordOrder.get(3)); // Grace (age
                                                                // 32)
            Assert.assertEquals(4L, (long) recordOrder.get(4)); // Diana (age
                                                                // 35)
            Assert.assertEquals(8L, (long) recordOrder.get(5)); // Henry (age
                                                                // 38)
            Assert.assertEquals(3L, (long) recordOrder.get(6)); // Charlie (age
                                                                // 40)
            Assert.assertEquals(6L, (long) recordOrder.get(7)); // Frank (age
                                                                // 45)

            // Verify correct data is returned for a sample record
            Map<String, Set<TObject>> record7 = data.get(7L);
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Grace")),
                    record7.get("name"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Austin")),
                    record7.get("city"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(90)),
                    record7.get("score"));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift(true)),
                    record7.get("active"));
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testPaginationWithOffset() {
        // Test pagination with offset.
        // This verifies that pagination works correctly with both offset and
        // limit.
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a page with offset 3 and limit 2
            Page page = Page.of(3, 2);

            // Select "name" key and order by "name"
            String key = "name";
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);
            long timestamp = Time.NONE;
            Order order = Order.by("name");
            Supplier<SortableColumn<Set<TObject>>> supplier = () -> SortableColumn
                    .multiValued(key, new LinkedHashMap<>(records.size()));

            Map<Long, Set<TObject>> data = Operations
                    .selectKeyRecordsOptionalAtomic(store, key, records,
                            timestamp, order, page, supplier);

            // Verify correct records are returned in correct order
            Assert.assertEquals(2, data.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(data.keySet());
            Assert.assertEquals(4L, (long) recordOrder.get(0)); // Diana (4th
                                                                // alphabetically)
            Assert.assertEquals(5L, (long) recordOrder.get(1)); // Eve (5th
                                                                // alphabetically)

            // Verify correct data is returned
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Diana")),
                    data.get(4L));
            Assert.assertEquals(ImmutableSet.of(Convert.javaToThrift("Eve")),
                    data.get(5L));
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testFrameRecordsAscendingOrderWithLimit() {
        // Setup the test environment
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a collection of records to frame
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);

            // Frame records by name in ascending order with pagination
            Order nameAscOrder = Order.by("name");
            Page page = Page.sized(3);

            Set<Long> framedRecords = Operations.frameRecordsOptionalAtomic(
                    store, records, nameAscOrder, page);

            // Verify correct records are returned in correct order
            Assert.assertEquals(3, framedRecords.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(framedRecords);
            Assert.assertEquals(1L, (long) recordOrder.get(0)); // Alice (first
                                                                // alphabetically)
            Assert.assertEquals(2L, (long) recordOrder.get(1)); // Bob (second
                                                                // alphabetically)
            Assert.assertEquals(3L, (long) recordOrder.get(2)); // Charlie
                                                                // (third
                                                                // alphabetically)
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testFrameRecordsDescendingOrderWithLimit() {
        // Setup the test environment
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a collection of records to frame
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);

            // Frame records by age in descending order with pagination
            Order ageDescOrder = Order.by("age").descending();
            Page page = Page.sized(4);

            Set<Long> framedRecords = Operations.frameRecordsOptionalAtomic(
                    store, records, ageDescOrder, page);

            // Verify correct records are returned in correct order
            Assert.assertEquals(4, framedRecords.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(framedRecords);
            Assert.assertEquals(6L, (long) recordOrder.get(0)); // Frank (age
                                                                // 45)
            Assert.assertEquals(3L, (long) recordOrder.get(1)); // Charlie (age
                                                                // 40)
            Assert.assertEquals(8L, (long) recordOrder.get(2)); // Henry (age
                                                                // 38)
            Assert.assertEquals(4L, (long) recordOrder.get(3)); // Diana (age
                                                                // 35)
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testFrameRecordsWithOffsetPagination() {
        // Setup the test environment
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a collection of records to frame
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);

            // Frame records with offset pagination
            Order scoreOrder = Order.by("score").descending();
            Page page = Page.of(2, 3); // Skip 2, take 3

            Set<Long> framedRecords = Operations.frameRecordsOptionalAtomic(
                    store, records, scoreOrder, page);

            // Verify correct records are returned in correct order
            Assert.assertEquals(3, framedRecords.size());

            // Convert to list to verify order
            List<Long> recordOrder = new ArrayList<>(framedRecords);
            Assert.assertEquals(7L, (long) recordOrder.get(0)); // Grace (score
                                                                // 90)
            Assert.assertEquals(4L, (long) recordOrder.get(1)); // Diana (score
                                                                // 88)
            Assert.assertEquals(1L, (long) recordOrder.get(2)); // Alice (score
                                                                // 85)
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testFrameRecordsWithNoPagination() {
        // Setup the test environment
        AtomicSupport store = getStore();
        try {
            setupDisconnectedGraph(store);

            // Create a collection of records to frame
            List<Long> records = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L,
                    8L);

            // Frame records with no pagination
            Order cityOrder = Order.by("city");
            Page page = Page.none();

            Set<Long> framedRecords = Operations.frameRecordsOptionalAtomic(
                    store, records, cityOrder, page);

            // Verify all records are returned in correct order
            Assert.assertEquals(8, framedRecords.size());

            // Convert to list to verify order (cities in alphabetical order)
            List<Long> recordOrder = new ArrayList<>(framedRecords);
            Assert.assertEquals(7L, (long) recordOrder.get(0)); // Austin
            Assert.assertEquals(2L, (long) recordOrder.get(1)); // Boston
            Assert.assertEquals(3L, (long) recordOrder.get(2)); // Chicago
            Assert.assertEquals(4L, (long) recordOrder.get(3)); // Denver
            Assert.assertEquals(6L, (long) recordOrder.get(4)); // Miami
            Assert.assertEquals(1L, (long) recordOrder.get(5)); // New York
            Assert.assertEquals(8L, (long) recordOrder.get(6)); // Portland
            Assert.assertEquals(5L, (long) recordOrder.get(7)); // Seattle
        }
        finally {
            store.stop();
        }
    }

    /**
     * Return an {@link AtomicSupport} {@link Store} that can be used in unit
     * tests.
     * 
     * @return an {@link AtomicSupport} store
     */
    protected AtomicSupport getStore() {
        String directory = TestData.DATA_DIR + File.separator + Time.now();
        Engine store = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        store.start();
        return store;
    }

}
