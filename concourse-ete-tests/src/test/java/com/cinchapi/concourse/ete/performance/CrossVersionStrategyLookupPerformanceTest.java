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
package com.cinchapi.concourse.ete.performance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Unit tests for various lookup scenarios across versions.
 *
 * @author Jeff Nelson
 */
@Versions({ "0.10.3", "latest" })
public class CrossVersionStrategyLookupPerformanceTest
        extends CrossVersionTest {

    @Test
    public void testFindSortKeyAndConditionKey() throws InterruptedException {
        init();
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
        record("strategy", avg);
    }

    @Test
    public void testSelectSortKeyAndConditionKey() throws InterruptedException {
        init();
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
        record("strategy", avg);
    }

    /**
     * Setup test data and restart the server (to flush cache).
     */
    private void init() {
        int count = 20000;
        List<Integer> counts = Lists.newArrayList();
        for (int i = 0; i < count; ++i) {
            counts.add(i);
        }
        Collections.shuffle(counts);
        counts.forEach(c -> {
            client.insert(
                    ImmutableMap.of("name", c, "count", c, "foo", "c", "b", c));
        });
        while (server.hasWritesToTransport()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {/* ignore */}
            continue;
        }
        server.stop();
        server.start();
        client = server.connect();
    }

}
