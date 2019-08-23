/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.ImmutableMap;

/**
 * Unit test for performance across versions.
 *
 * @author Jeff Nelson
 */
@Versions({ "0.9.6", "0.10.1", "latest" })
public class CrossVersionPerformanceBenchmarkTest extends CrossVersionTest {

    @Test
    public void testWrite() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                for (int i = 0; i < 10000; ++i) {
                    client.add("foo", Time.now(), i);
                }
            }

        };
        double avg = benchmark.run(10) / 10;
        record("write", avg);
    }

    @Test
    public void testRead() {
        for (int i = 0; i < 10000; ++i) {
            client.insert(ImmutableMap.of("foo", Time.now(), "bar", true, "baz",
                    "hello", "bang", Tag.create("mafia")));
        }
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select("foo < " + Time.now());
            }

        };
        double avg = benchmark.run(10) / 10;
        record("read", avg);
    }
}
