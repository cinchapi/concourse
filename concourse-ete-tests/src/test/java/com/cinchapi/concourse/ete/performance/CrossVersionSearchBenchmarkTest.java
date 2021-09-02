/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.util.Random;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Unit test for search performance across versions.
 *
 * @author Jeff Nelson
 */
public class CrossVersionSearchBenchmarkTest extends CrossVersionBenchmarkTest {

    static String key;
    static String query;
    static List<String> values;
    static {
        key = Random.getSimpleString();
        values = Lists.newArrayList();
        int count = 100000;
        for (int i = 0; i < count; ++i) {
            String value = null;
            while (Strings.isNullOrEmpty(value)) {
                value = Random.getString();
            }
            values.add(value);
        }
        query = null;
        while (Strings.isNullOrEmpty(query) || query.length() < 5) {
            query = values.get(Math.abs(Random.getInt()) % values.size());
            int start = Math.abs(Random.getInt()) % query.length();
            int end = Math.min(query.length(),
                    start + (Math.abs(Random.getInt()) % query.length()));
            query = query.substring(start, end).trim();
        }
    }

    @Test
    public void testColdSearchPerformance() {
        client.close();
        server.stop();
        server.start();
        client = server.connect();
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.search(key, query);
            }

        };
        double avg = benchmark.average(10);
        record("cold", avg);
    }

    @Test
    public void testWarmSearchPerformance() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.search(key, query);
            }

        };
        double avg = benchmark.average(10);
        record("warm", avg);
    }

    @Override
    protected void beforeEachBenchmarkRuns() {
        for (int i = 0; i < values.size(); ++i) {
            String value = values.get(i);
            client.add(key, value, i);
        }
    }

}
