/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * The base class for unit tests that benchmark performance.
 * <p>
 * <strong>NOTE:</strong> This framework should not be used for measuring
 * precise performance metrics. Its really designed to provide relative
 * benchmarking to give a general sense about how a unit performs.
 * </p>
 * <p>
 * You can assert that one benchmark is faster than another one by using the
 * {@link #assertFasterThan(String, String)} method.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ConcourseBenchmarkTest extends ConcourseBaseTest {

    /**
     * A collection from keys to stopwatches that encapsulates various
     * benchmarks that are taken in the test methods.
     */
    private final Map<String, Stopwatch> benchmarks = Maps.newHashMap();

    @Override
    public void afterEachTest() {
        TimeUnit unit = desiredTimeUnit();
        for (Map.Entry<String, Stopwatch> entry : benchmarks.entrySet()) {
            System.out.println("Benchmark " + entry.getKey() + " took "
                    + entry.getValue().elapsed(unit) + " "
                    + unit.name().toLowerCase());
        }
        super.afterEachTest();
    }

    /**
     * Assert that the {@code faster} benchmark ran in less time than the
     * {@code slower} benchmark.
     * 
     * @param faster the name of the benchmark that is expected to be faster
     * @param slower the name of the benchmark that is expected to be slower
     */
    protected void assertFasterThan(String faster, String slower) {
        Stopwatch f = benchmarks.get(faster);
        Stopwatch s = benchmarks.get(slower);
        TimeUnit unit = desiredTimeUnit();
        Preconditions.checkArgument(f != null, "% is not a valid benchmark",
                faster);
        Preconditions.checkArgument(s != null, "% is not a valid benchmark",
                slower);
        Assert.assertTrue(f.elapsed(unit) < s.elapsed(unit));

    }

    /**
     * Return the {@link TimeUnit} that should be used when displaying the
     * results of the benchmark and comparing the benchmark to others.
     * <p>
     * The default unit is {@link TimeUnit#MILLISECONDS milliseconds}.
     * </p>
     * 
     * @return the desired {@link TimeUnit}
     */
    protected TimeUnit desiredTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    /**
     * Start running the benchmark called {@code name}.
     * 
     * <pre>
     * &#064;Test
     * public void testFoo() {
     *     startBenchmark(&quot;foo&quot;);
     *     // ... benchmark code
     *     stopBenchmark(&quot;foo&quot;);
     * }
     * </pre>
     * 
     * @param name the name of the benchmark
     */
    protected void startBenchmark(String name) {
        Stopwatch watch = Stopwatch.createUnstarted();
        benchmarks.put(name, watch);
        watch.start();
    }

    /**
     * Stop running the benchmark called {@code name}.
     * 
     * <pre>
     * &#064;Test
     * public void testFoo() {
     *     startBenchmark(&quot;foo&quot;);
     *     // ... benchmark code
     *     stopBenchmark(&quot;foo&quot;);
     * }
     * </pre>
     * 
     * @param name the name of the benchmark
     */
    protected void stopBenchmark(String name) {
        Stopwatch watch = benchmarks.get(name);
        watch.stop();
        System.gc();
    }

}
