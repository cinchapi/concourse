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
package com.cinchapi.concourse.performance;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Performance tests for functionality introduced in version 0.10.
 *
 * @author Jeff Nelson
 */
public class Version0_10PerformanceTest extends UpgradeTest {

    private Set<Long> records = Sets.newHashSet();
    private long legacy;
    private long latest;

    @Test
    public void testNoPerformanceRegressionFromNoOpOrderEvaluation() {
        // In version 0.10.0, the implementation of non-order imposing methods
        // (i.e. selectRecords) was changed to call into order-imposing
        // counterparts (i.e. selectRecordsOrder) with special order ignoring
        // parameters. This method verifies that no additional overhead is added
        // from this approach.
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(records);
            }

        };
        latest = benchmark.run();
        int legacyTimeDigits = (int) Math.ceil(Math.log10(legacy));
        int latestTimeDigits = (int) Math.ceil(Math.log10(latest));
        System.out.println(AnyStrings.format("Legacy = {} {} with {} digits",
                legacy, TimeUnit.MILLISECONDS, legacyTimeDigits));
        System.out.println(AnyStrings.format("Latest = {} {} with {} digits",
                latest, TimeUnit.MILLISECONDS, latestTimeDigits));
        Assert.assertTrue(
                AnyStrings.format("Legacy took {} {} and Latest took {} {}",
                        legacy, TimeUnit.MILLISECONDS, latest,
                        TimeUnit.MILLISECONDS),
                legacyTimeDigits >= latestTimeDigits);
    }

    @Override
    protected String getInitialServerVersion() {
        return "0.9.6";
    }

    @Override
    protected void preUpgradeActions() {
        java.util.Random random = new java.util.Random();
        int count = Random.getScaleCount();
        Set<String> keys = Sets.newHashSet();
        for (int i = 0; i < count; ++i) {
            keys.add(Random.getSimpleString());
        }
        for (int i = 0; i < count; ++i) {
            records.add((long) i);
            int count2 = random.nextInt(keys.size());
            for (int j = 0; j < count2; ++j) {
                String key = Iterables.get(keys, random.nextInt(keys.size()));
                client.add(key, Random.getObject(), i);
            }
        }
        server.stop();
        server.start();
        client = server.connect();
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(records);
            }

        };
        legacy = benchmark.run();
    }

}
