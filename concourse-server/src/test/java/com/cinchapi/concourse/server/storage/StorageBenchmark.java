/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.util.TestData;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class StorageBenchmark extends AbstractBenchmark {

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 10)
    public void benchmarkWriteAdd() {
        Write.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 10)
    public void benchmarkWriteRemove() {
        Write.remove(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 10)
    public void benchmarkWriteNotStorable() {
        Write.notStorable(TestData.getString(), TestData.getTObject(),
                TestData.getLong());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 10)
    public void benchmarkBufferInsert() {
        // TODO
    }

}
