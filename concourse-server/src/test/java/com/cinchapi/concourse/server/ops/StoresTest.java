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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Stores}.
 *
 * @author Jeff Nelson
 */
public class StoresTest {

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
