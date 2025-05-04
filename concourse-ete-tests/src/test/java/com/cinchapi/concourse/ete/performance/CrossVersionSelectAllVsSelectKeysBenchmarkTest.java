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
package com.cinchapi.concourse.ete.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for benchmarking select multiple keys compared to select all.
 *
 * @author Jeff Nelson
 */
public class CrossVersionSelectAllVsSelectKeysBenchmarkTest
        extends CrossVersionBenchmarkTest {

    Map<String, Object> data = ImmutableMap.of("name", "Jeff Nelson", "age", 34,
            "Company", "Cinchapi", "active", true, "title", "Founder and CEO");
    List<Long> records = new ArrayList<>();
    {
        for (long i = 1; i <= 20000; ++i) {
            records.add(i);
        }
    }

    @Override
    protected void beforeEachBenchmarkRuns() {
        for (long record : records) {
            client.insert(data, record);
        }
    }

    @Test
    public void testSelectAll() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(records);
            }

        };
        long elapsed = benchmark.run();
        record("select all", elapsed);
    }

    @Test
    public void testSelectKeys() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(ImmutableList.of("name", "age", "company",
                        "active", "title"), records);
            }

        };
        long elapsed = benchmark.run();
        record("select keys", elapsed);
    }

}
