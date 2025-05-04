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
package com.cinchapi.concourse.server.ops;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.ops.Stores.NavigationKeyFinder;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.TestData;
import com.cinchapi.concourse.validate.Keys;
import com.cinchapi.concourse.validate.Keys.Key;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Stores}.
 *
 * @author Jeff Nelson
 */
public class StoresTest {

    @Test
    public void testBenchmarkSelectKeyRecordsOptionalAtomicWithNavigation() {
        AtomicSupport store = getStore();
        setupNavigationGraph(store);
        // @formatter:off
        List<String> keys = ImmutableList.of(
                "job.title"
        );
        // @formatter:on
        Benchmark serial = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.serialSelect(store, keys.get(0), 1);
            }

        };
        Benchmark bulk = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.select(store, keys, 1);
            }

        };

        double serialTime = serial.average(5);
        double bulkTime = bulk.average(5);

        System.out.println("single navigation serial = " + serialTime);
        System.out.println("single navigation BULK = " + bulkTime);
    }

    @Test
    public void testBenchmarkSelectKeyRecordsOptionalAtomicWithNoNavigation() {
        AtomicSupport store = getStore();
        setupNavigationGraph(store);
        // @formatter:off
        List<String> keys = ImmutableList.of(
                "job"
        );
        // @formatter:on
        Benchmark serial = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.serialSelect(store, keys.get(0), 1);
            }

        };
        Benchmark bulk = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.select(store, keys, 1);
            }

        };

        double serialTime = serial.average(5);
        double bulkTime = bulk.average(5);

        System.out.println("single serial = " + serialTime);
        System.out.println("single BULK = " + bulkTime);
    }

    @Test
    public void testBenchmarkSelectKeysRecordsOptionalAtomicWithNavigation() {
        AtomicSupport store = getStore();
        setupNavigationGraph(store);
        // @formatter:off
        List<String> keys = ImmutableList.of(
                "job.title",
                "job.company.name",
                "student.name",
                "student.email",
                "job.company.website",
                "association",
                "foo.bar",
                "student.association",
                "job.company.$id$",
                "student.name.$id$",
                "job.company"
        );
        // @formatter:on
        Benchmark serial = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                for (String key : keys) {
                    Stores.serialSelect(store, key, 1);
                }
            }

        };
        Benchmark bulk = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.select(store, keys, 1);
            }

        };

        double serialTime = serial.average(5);
        double bulkTime = bulk.average(5);

        System.out.println("multiple navigation serial = " + serialTime);
        System.out.println("multiple navigation BULK = " + bulkTime);
    }

    @Test
    public void testBenchmarkSelectKeysRecordsOptionalAtomicWithNoNavigation() {
        AtomicSupport store = getStore();
        setupNavigationGraph(store);
        // @formatter:off
        List<String> keys = ImmutableList.of(
                "job",
                "student",
                "association",
                "foo"
        );
        // @formatter:on
        Benchmark serial = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                for (String key : keys) {
                    Stores.serialSelect(store, key, 1);
                }
            }

        };
        Benchmark bulk = new Benchmark(TimeUnit.MICROSECONDS) {

            @Override
            public void action() {
                Stores.select(store, keys, 1);
            }

        };

        double serialTime = serial.average(5);
        double bulkTime = bulk.average(5);

        System.out.println("multiple serial = " + serialTime);
        System.out.println("multiple BULK = " + bulkTime);

    }

    /**
     * Test navigation with a deep path that has multiple levels.
     */
    @Test
    public void testFindDeepNavigation() {
        AtomicSupport store = getStore();
        int count = 20;
        Map<String, List<Long>> recordIds = setupComplexNavigationGraph(store,
                count);

        // Add an extra level to the navigation path
        for (Long credentialId : recordIds.get("credentials")) {
            long profileId = credentialId * 10;
            store.accept(Write.add("profile",
                    Convert.javaToThrift(Link.to(profileId)), credentialId));
            store.accept(Write.add("verified", Convert.javaToThrift(true),
                    profileId));
        }

        // Find users by profile verification status through a 4-level deep path
        String key = "identity.credential.profile.verified";
        TObject value = Convert.javaToThrift(true);

        Set<Long> results = Stores.find(store, key, Operator.EQUALS, value);

        // Should find all users since all profiles are verified
        Assert.assertEquals(count, results.size());
    }

    @Test
    public void testFindNavigationKey() {
        AtomicSupport store = getStore();
        setupComplexNavigationGraph(store, 100);
        String key = "identity.credential.email";
        TObject value = Convert.javaToThrift("email17");
        Operator operator = Operator.EQUALS;
        Set<Long> expected = Stores.findNavigationKey(
                NavigationKeyFinder.ADHOC_INDEX, store, Time.NONE,
                Keys.parse(key), operator, value);
        Set<Long> actual = Stores.find(store, key, Operator.EQUALS, value);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testForwardTraversalNavigation() {
        AtomicSupport auto = getStore();
        AtomicSupport legacy = getStore();
        AtomicSupport forward = getStore();
        AtomicSupport reverse = getStore();
        int count = 100;

        setupComplexNavigationGraph(auto, count, false);
        setupComplexNavigationGraph(legacy, count, false);
        setupComplexNavigationGraph(forward, count, false);
        setupComplexNavigationGraph(reverse, count, false);

        // Find users whose credentials have counter > 10. This should match
        // many credentials
        Key key = Keys.parse("identity.credential.counter");
        Operator operator = Operator.GREATER_THAN;
        TObject value = Convert.javaToThrift(10);

        Set<Long> expected = Stores.findNavigationKey(
                NavigationKeyFinder.ADHOC_INDEX, legacy, Time.NONE, key,
                operator, value);
        Set<Long> actual = Stores.findNavigationKey(NavigationKeyFinder.AUTO,
                legacy, Time.NONE, key, operator, value);
        Assert.assertEquals(expected, actual);

        CompletableFuture<Double> legacyTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.ADHOC_INDEX, legacy, Time.NONE, key,
                        operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        CompletableFuture<Double> forwardTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.FORWARD_TRAVERSAL, legacy,
                        Time.NONE, key, operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        CompletableFuture<Double> reverseTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.REVERSE_TRAVERSAL, legacy,
                        Time.NONE, key, operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        System.out.println("Forward Traversal Navigation");
        System.out.println("-----------------------------");
        System.out.println("legacy time = " + legacyTime.join());
        System.out.println("forward time = " + forwardTime.join());
        System.out.println("reverse time = " + reverseTime.join());
        System.out.println("-----------------------------");
    }

    /**
     * Test reverse traversal navigation.
     * This test creates a scenario where reverse traversal is preferred
     * because:
     * - There is only one record with the target value (one specific counter
     * value)
     * - But many records at the start of the path
     */
    @Test
    public void testReverseTraversalNavigation() {
        AtomicSupport auto = getStore();
        AtomicSupport legacy = getStore();
        AtomicSupport forward = getStore();
        AtomicSupport reverse = getStore();
        int count = 100;

        setupComplexNavigationGraph(auto, count, false);
        setupComplexNavigationGraph(legacy, count, false);
        setupComplexNavigationGraph(forward, count, false);
        setupComplexNavigationGraph(reverse, count, false);

        // Only one credential has this specific counter value
        Key key = Keys.parse("identity.credential.counter");
        Operator operator = Operator.EQUALS;
        TObject value = Convert.javaToThrift(50);

        Set<Long> expected = Stores.findNavigationKey(
                NavigationKeyFinder.ADHOC_INDEX, legacy, Time.NONE, key,
                operator, value);
        Set<Long> actual = Stores.findNavigationKey(NavigationKeyFinder.AUTO,
                legacy, Time.NONE, key, operator, value);
        Assert.assertEquals(expected, actual);

        CompletableFuture<Double> legacyTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.ADHOC_INDEX, legacy, Time.NONE, key,
                        operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        CompletableFuture<Double> forwardTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.FORWARD_TRAVERSAL, legacy,
                        Time.NONE, key, operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        CompletableFuture<Double> reverseTime = Benchmark
                .measure(() -> Stores.findNavigationKey(
                        NavigationKeyFinder.REVERSE_TRAVERSAL, legacy,
                        Time.NONE, key, operator, value))
                .in(TimeUnit.MICROSECONDS).warmups(1).async().average(5);

        System.out.println("Reverse Traversal Navigation");
        System.out.println("-----------------------------");
        System.out.println("legacy time = " + legacyTime.join());
        System.out.println("forward time = " + forwardTime.join());
        System.out.println("reverse time = " + reverseTime.join());
        System.out.println("-----------------------------");
    }

    @Test
    public void testSelectKeysRecordsOptionalAtomicWithNavigation() {
        AtomicSupport store = getStore();
        setupNavigationGraph(store);
        // @formatter:off
        List<String> keys = ImmutableList.of(
                "job.title",
                "job.company.name",
                "student.name",
                "student.email",
                "job.company.website",
                "association",
                "foo.bar",
                "student.association",
                "job.company.$id$",
                "student.name.$id$",
                "job.company"
        );
        // @formatter:on
        Map<String, Set<TObject>> _actual = Stores.select(store, keys, 1);
        Map<Long, Map<String, Set<TObject>>> actual = PrettyLinkedTableMap
                .of(ImmutableMap.of(1L, _actual));
        System.out.println(actual);
        Map<String, Set<TObject>> _expected = new LinkedHashMap<>();
        for (String key : keys) {
            _expected.put(key, Stores.serialSelect(store, key, 1));
        }
        Map<Long, Map<String, Set<TObject>>> expected = PrettyLinkedTableMap
                .of(ImmutableMap.of(1L, _expected));
        System.out.println(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    @Ignore
    public void testBulkSelectDuringTransports() throws InterruptedException {
        AtomicSupport store = getStore();
        Buffer buffer = Reflection.get("limbo", store);

        AtomicBoolean transportable = new AtomicBoolean(false);
        while (!transportable.get()) {
            for (int i = 0; i < TestData.getScaleCount(); ++i) {
                store.accept(TestData.getWriteAdd());
            }
            if(!transportable.get()) {
                transportable.set(Reflection.call(buffer, "canTransport"));
            }
        }
        setupNavigationGraph(store);
        List<String> keys = ImmutableList.of("name", "email", "title",
                "website", "association");

        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch start = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            while (!stop.get()) {
                for (int i = 0; i < TestData.getScaleCount(); ++i) {
                    store.accept(TestData.getWriteAdd());
                }
                Stores.select(store, keys, 1);
                start.countDown();
            }
        });
        writer.start();
        start.await();

        Map<String, Set<TObject>> actual = Stores.select(store, keys, 1);
        Map<String, Set<TObject>> expected = new LinkedHashMap<>();
        for (String key : keys) {
            expected.put(key, Stores.serialSelect(store, key, 1));
        }

        Assert.assertEquals(expected, actual);
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

    /**
     * Overloaded method that defaults to one-to-one relationships
     */
    protected Map<String, List<Long>> setupComplexNavigationGraph(
            AtomicSupport store, int count) {
        return setupComplexNavigationGraph(store, count, false);
    }

    /**
     * Sets up a complex navigation graph with the specified number of entries.
     * The graph has the following structure:
     * - n user records, each with a unique name ("user"+i)
     * - Each user links to a distinct identity record via "identity" key
     * - Each identity has a unique id ("identity"+i) and links to a credential
     * record via "credential" key
     * - Each credential has a unique email ("email"+i)
     * - All records have a "counter" field with their index value
     *
     * This creates a three-level deep navigation structure: user -> identity ->
     * credential
     * 
     * @param store the AtomicSupport store to populate
     * @param count the number of entries to create in the graph
     * @param manyToOne if true, creates a many-to-one relationship where
     *            multiple users point to the same identity
     * @return a map of record IDs for reference (keys: "users", "identities",
     *         "credentials")
     */
    protected Map<String, List<Long>> setupComplexNavigationGraph(
            AtomicSupport store, int count, boolean manyToOne) {
        List<Long> userIds = new ArrayList<>(count);
        List<Long> identityIds = new ArrayList<>();
        List<Long> credentialIds = new ArrayList<>();

        // For many-to-one testing, we'll use fewer identities and credentials
        int identityCount = manyToOne ? Math.max(count / 10, 1) : count;
        int credentialCount = manyToOne ? Math.max(count / 20, 1) : count;

        // Create all credential records first
        for (int i = 0; i < credentialCount; i++) {
            long credentialId = (i + 1) * 100 + 2;
            credentialIds.add(credentialId);
            store.accept(Write.add("email", Convert.javaToThrift("email" + i),
                    credentialId));
            store.accept(Write.add("counter", Convert.javaToThrift(i),
                    credentialId));
            store.accept(Write.add("tag",
                    Convert.javaToThrift("credential-tag"), credentialId));
        }

        // Create all identity records
        for (int i = 0; i < identityCount; i++) {
            long identityId = (i + 1) * 100 + 1;
            identityIds.add(identityId);
            store.accept(Write.add("id", Convert.javaToThrift("identity" + i),
                    identityId));
            store.accept(
                    Write.add("counter", Convert.javaToThrift(i), identityId));

            // Link to a credential (in many-to-one, multiple identities may
            // link to same credential)
            long credentialId = credentialIds.get(i % credentialCount);
            store.accept(Write.add("credential",
                    Convert.javaToThrift(Link.to(credentialId)), identityId));

            store.accept(Write.add("tag", Convert.javaToThrift("identity-tag"),
                    identityId));
        }

        // Create all user records
        for (int i = 0; i < count; i++) {
            long userId = (i + 1) * 100;
            userIds.add(userId);
            store.accept(Write.add("name", Convert.javaToThrift("user" + i),
                    userId));
            store.accept(Write.add("counter", Convert.javaToThrift(i), userId));

            // Link to an identity (in many-to-one, multiple users may link to
            // same identity)
            long identityId = identityIds.get(i % identityCount);
            store.accept(Write.add("identity",
                    Convert.javaToThrift(Link.to(identityId)), userId));

            store.accept(
                    Write.add("tag", Convert.javaToThrift("user-tag"), userId));
        }

        Map<String, List<Long>> recordIds = new HashMap<>();
        recordIds.put("users", userIds);
        recordIds.put("identities", identityIds);
        recordIds.put("credentials", credentialIds);

        return recordIds;
    }

    protected void setupNavigationGraph(AtomicSupport store) {
        long offer = 1;
        long student = 2;
        long job = 3;
        long company = 4;
        long company2 = 7;
        long association = 5;
        long details = 6;
        store.accept(Write.add("name", Convert.javaToThrift("Association"),
                association));
        store.accept(
                Write.add("name", Convert.javaToThrift("Company"), company));
        store.accept(
                Write.add("website", Convert.javaToThrift("Website"), company));
        store.accept(
                Write.add("name", Convert.javaToThrift("Company2"), company2));
        store.accept(Write.add("website", Convert.javaToThrift("Website"),
                company2));
        store.accept(
                Write.add("name", Convert.javaToThrift("Student"), student));
        store.accept(Write.add("name", Convert.javaToThrift("Good Student"),
                student));
        store.accept(
                Write.add("email", Convert.javaToThrift("Email"), student));
        store.accept(Write.add("title", Convert.javaToThrift("Title"), job));
        store.accept(Write.add("company",
                Convert.javaToThrift(Link.to(company)), job));
        store.accept(Write.add("company",
                Convert.javaToThrift(Link.to(company2)), job));
        store.accept(Write.add("association",
                Convert.javaToThrift(Link.to(association)), offer));
        store.accept(
                Write.add("job", Convert.javaToThrift(Link.to(job)), offer));
        store.accept(Write.add("student",
                Convert.javaToThrift(Link.to(student)), offer));
        store.accept(Write.add("details",
                Convert.javaToThrift(Link.to(details)), offer));
    }

}
