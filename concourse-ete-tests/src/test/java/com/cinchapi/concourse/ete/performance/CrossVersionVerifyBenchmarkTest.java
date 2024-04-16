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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.ImmutableMap;

/**
 * Benchmark
 * {@link com.cinchapi.concourse.Concourse#verify(String, Object, long) verify}
 * performance across versions.
 *
 * @author Jeff Nelson
 */
public class CrossVersionVerifyBenchmarkTest extends CrossVersionBenchmarkTest {

    /**
     * Data to use in each verion's test. Defined statically to prevent GC
     * between test runs.
     */
    static List<Map<String, Object>> data = new ArrayList<>(10000);
    static Tag tag = Tag.create("mafia");
    static {
        for (int i = 0; i < 10000; ++i) {
            data.add(ImmutableMap.of("foo", Time.now(), "bar", true, "baz",
                    "hello", "bang", tag));
        }
    }

    @Override
    protected void beforeEachBenchmarkRuns() {
        for (int i = 0; i < data.size(); ++i) {
            Map<String, Object> map = data.get(i);
            client.insert(map, i + 1);
        }
    }

    @Test
    public void testVerify() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                for (int i = 0; i < 10000; ++i) {
                    // Verify known hits
                    client.verify("baz", "hello", i + 1);
                    client.verify("bang", tag, i + 1);

                    // Verify known miss
                    client.verify("foo", Time.now(), i + 1);
                }
            }

        };
        double avg = benchmark.average(10);
        record("verify", avg);
    }

}
