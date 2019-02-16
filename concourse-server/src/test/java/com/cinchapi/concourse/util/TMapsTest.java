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
package com.cinchapi.concourse.util;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Test for map based utility methods that are not found in the Guava
 * {@link com.google.collect.Maps} utility class.
 * 
 * @author Jeff Nelson
 */
public class TMapsTest {

    @Test
    public void testTObjectResultDatasetPutPerformance()
            throws InterruptedException {
        // Constraint: Putting into a Dataset can't be more than an order of
        // magnitude slower...
        Map<Long, Map<String, Set<TObject>>> spec = Maps.newLinkedHashMap();
        int rounds = 1000;
        TimeUnit unit = TimeUnit.MICROSECONDS;
        for (int i = 0; i < rounds; ++i) {
            String key = Random.getSimpleString();
            Set<TObject> values = Sets.newLinkedHashSet();
            for (int j = 0; j < 10; ++j) {
                values.add(Convert.javaToThrift(Random.getObject()));
            }
            Map<String, Set<TObject>> entry = Maps
                    .newLinkedHashMapWithExpectedSize(1);
            entry.put(key, values);
            spec.put((long) i, entry);
        }
        Map<Long, Map<String, Set<TObject>>> map = Maps.newLinkedHashMap();
        Map<Long, Map<String, Set<TObject>>> dataset = new TObjectResultDataset();
        Benchmark mapBench = new Benchmark(unit) {

            @Override
            public void action() {
                spec.forEach((record, data) -> {
                    TMaps.putResultDatasetOptimized(map, record, data);
                });
            }

        };

        Benchmark datasetBench = new Benchmark(unit) {

            @Override
            public void action() {
                spec.forEach((record, data) -> {
                    TMaps.putResultDatasetOptimized(dataset, record, data);
                });

            }

        };

        AtomicLong datasetTime = new AtomicLong(0);
        AtomicLong mapTime = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            datasetTime.set(datasetBench.run());
            latch.countDown();
        });
        Thread t2 = new Thread(() -> {
            mapTime.set(mapBench.run());
            latch.countDown();
        });
        t2.start();
        t1.start();
        latch.await();
        double datasetTimeDigits = Math.ceil(Math.log10(datasetTime.get()));
        double mapTimeDigits = Math.ceil(Math.log10(mapTime.get()));
        System.out.println(AnyStrings.format("Dataset = {} {} with {} digits",
                datasetTime.get(), unit, datasetTimeDigits));
        System.out.println(AnyStrings.format("Map = {} {} with {} digits",
                mapTime.get(), unit, mapTimeDigits));
        Assert.assertTrue(
                AnyStrings.format("Datset took {} {} and Map took {} {}",
                        datasetTime.get(), unit, mapTime.get(), unit),
                datasetTimeDigits - mapTimeDigits <= 1);

    }

}
