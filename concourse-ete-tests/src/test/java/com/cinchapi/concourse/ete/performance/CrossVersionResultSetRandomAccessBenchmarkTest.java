/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.google.common.collect.ImmutableMap;

/**
 * Unit test for random access of result sets.
 *
 * @author Jeff Nelson
 */
public class CrossVersionResultSetRandomAccessBenchmarkTest
        extends CrossVersionBenchmarkTest {

    @Override
    protected void beforeEachBenchmarkRuns() {
        for (int i = 0; i < 1000; ++i) {
            Map<String, Object> data = ImmutableMap.of("count", i, "name",
                    "name" + i, "age", 100 + i, "test", true);
            client.insert(data);
        }
    }

    @Test
    public void testRandomAccess() {
        Set<Long> records = client.inventory();
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                Map<Long, Map<String, Set<Object>>> data = client
                        .select(records);
                for (long record : records) {
                    data.get(record);
                }
            }

        };
        long elapsed = benchmark.run();
        record("random access", elapsed);

    }

}
