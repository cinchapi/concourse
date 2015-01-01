/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.model;

import org.cinchapi.concourse.util.TestData;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

/**
 * 
 * 
 * @author jnelson
 */
public class ModelBenchmark extends AbstractBenchmark {

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 0)
    public void benchmarkValue() {
        Value.wrap(TestData.getTObject());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 0)
    public void benchmarkPrimaryKey() {
        PrimaryKey.wrap(TestData.getLong());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 0)
    public void benchmarkText() {
        Text.wrap(TestData.getString());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1000, warmupRounds = 0)
    public void benchmarkPosition() {
        Position.wrap(TestData.getPrimaryKey(), Math.abs(TestData.getInt()));
    }

}
