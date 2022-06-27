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
package com.cinchapi.concourse.ete.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * Benchmark for sorting various kinds of result sets
 *
 * @author Jeff Nelson
 */
public class CrossVersionSortPerformanceBenchmarkTest
        extends CrossVersionBenchmarkTest {

    static List<Multimap<String, Object>> data;
    static {
        data = new ArrayList<>();
        for (int i = 0; i < 5000; ++i) {
            Multimap<String, Object> row = ImmutableMultimap.of("name",
                    Random.getString(), "age", Random.getNumber(), "foo",
                    Random.getBoolean(), "bar", Random.getString(), "include", true);
            data.add(row);
        }
    }


    @Override
    protected void beforeEachBenchmarkRuns() {
        client.insert(data);
    }
    
    @Test
    public void testSortColumn() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select("name", "include = true", Order.by("name"));
            }
            
        };
        long elapsed = benchmark.run();
        record("column", elapsed);
    }

}
