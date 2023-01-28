/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for various lookup scenarios across versions.
 *
 * @author Jeff Nelson
 */
public class CrossVersionReadStrategyBenchmarkTest
        extends CrossVersionBenchmarkTest {

    public static String[] versions() {
        String[] versions = CrossVersionBenchmarkTests.VERSIONS;
        Set<String> $versions = Sets.newHashSet(versions);
        $versions.remove("0.9.6"); // sorting not supported
        versions = $versions.toArray(Array.containing());
        return versions;

    }

    @Test
    public void testFindSortKeyAndConditionKey() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.find(
                        Criteria.where().key("count")
                                .operator(Operator.GREATER_THAN).value(672),
                        Order.by("count"));
            }

        };
        double avg = benchmark.run(1);
        record("testFindSortKeyAndConditionKey", avg);
    }

    @Test
    public void testSelectSortKeyAndConditionKey() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select("count",
                        Criteria.where().key("count")
                                .operator(Operator.GREATER_THAN).value(672),
                        Order.by("count"));
            }

        };
        double avg = benchmark.run(1);
        record("testSelectSortKeyAndConditionKey", avg);
    }

    @Test
    public void testSelectManyKeysSortKey() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(Lists.newArrayList("count", "foo", "c"),
                        client.inventory(), Order.by("count"));
            }

        };
        double avg = benchmark.run(1);
        record("testSelectManyKeysSortKey", avg);
    }

    @Test
    public void testSelectAllKeysSortKey() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(client.inventory(), Order.by("count"));
            }

        };
        double avg = benchmark.run(1);
        record("testSelectAllKeysSortKey", avg);
    }

    @Test
    public void testSelectDiffKeyFromSortKey() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(Lists.newArrayList("name", "c"),
                        client.inventory(), Order.by("count"));
            }

        };
        double avg = benchmark.run(1);
        record("testSelectDiffKeyFromSortKey", avg);
    }

    private static List<Map<String, Object>> data = new ArrayList<>(20000);
    static {
        int count = 20000;
        List<Integer> counts = Lists.newArrayList();
        for (int i = 0; i < count; ++i) {
            counts.add(i);
        }
        Collections.shuffle(counts);
        counts.forEach(c -> {
            data.add(
                    ImmutableMap.of("name", c, "count", c, "foo", "c", "b", c));
        });
    }

    @Override
    protected void beforeEachBenchmarkRuns() {
        data.forEach(map -> {
            client.insert(map);
        });
        while (server.hasWritesToTransport()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {/* ignore */}
            continue;
        }
        client.close();
        server.stop();
        server.start();
        client = server.connect();
    }

}
