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
package com.cinchapi.concourse.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link CompiledInfingram}.
 *
 * @author Jeff Nelson
 */
public class CompiledInfingramTest extends InfingramTest {

    @Override
    protected Infingram createInfingram(String string) {
        return new CompiledInfingram(string);
    }

    @Test
    public void testPerformance() throws InterruptedException {
        String needle = TestData.getSimpleString();
        List<String> haystacks = new ArrayList<>();
        for (int i = 0; i < TestData.getScaleCount() * 5; ++i) {
            String haystack = TestData.getString();
            if(Random.getInt() % 4 == 0) {
                haystack += needle + haystack;
            }
            haystacks.add(haystack);
        }
        Function<Infingram, Benchmark> func = infingram -> {
            return new Benchmark(TimeUnit.MILLISECONDS) {

                @Override
                public void action() {
                    for (String haystack : haystacks) {
                        infingram.in(haystack);
                    }
                }

            };
        };
        List<Infingram> options = ImmutableList.of(new Infingram(needle),
                new CompiledInfingram(needle));
        List<Thread> threads = new ArrayList<>();
        for (Infingram option : options) {
            threads.add(new Thread(() -> {
                Benchmark benchmark = func.apply(option);
                double time = benchmark.average(5);
                System.out.println(AnyStrings.format("{} took {} ms",
                        option.getClass(), time));
            }));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

}
