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
package com.cinchapi.concourse.server.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.collect.CloseableIterator;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * Unit tests for the
 * {@link com.cinchapi.concourse.server.io.ByteableCollections} util class.
 *
 * @author Jeff Nelson
 */
public class ByteableCollectionsTest extends ConcourseBaseTest {

    @Test
    public void testStreamIterator() {
        Path file = Paths.get(TestData.getTemporaryTestFile());
        List<PrimaryKey> values = Lists.newArrayList();
        int count = 10;
        for (int i = 0; i < count; ++i) {
            values.add(PrimaryKey.wrap(i));
        }
        ByteBuffer bytes = ByteableCollections.toByteBuffer(values);
        FileSystem.writeBytes(bytes, file.toString());
        int bufferSize = 64;
        Iterator<ByteBuffer> it = ByteableCollections.stream(file, bufferSize);
        List<PrimaryKey> newValues = Lists.newArrayList();
        while (it.hasNext()) {
            newValues.add(PrimaryKey.fromByteBuffer(it.next()));
        }
        Assert.assertEquals(values, newValues);
    }

    @Test
    public void testStreamingIterator() {
        Path file = Paths.get(TestData.getTemporaryTestFile());
        List<Value> values = Lists.newArrayList();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            values.add(TestData.getValue());
        }
        ByteBuffer bytes = ByteableCollections.toByteBuffer(values);
        FileSystem.writeBytes(bytes, file.toString());
        int bufferSize = TestData.getScaleCount();
        Iterator<ByteBuffer> it = ByteableCollections.stream(file, bufferSize);
        List<Value> newValues = Lists.newArrayList();
        while (it.hasNext()) {
            newValues.add(Value.fromByteBuffer(it.next()));
        }
        Assert.assertEquals(values, newValues);
    }

    @Test
    public void testNewVsDeprecatedPerformance()
            throws IOException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        List<Value> values = Lists.newArrayList();
        while (baos.size() < Math.pow(2, 24)) {
            Value value = TestData.getValue();
            baos.write(ByteBuffers.getByteArray(value.getBytes()));
            values.add(value);
        }
        ByteBuffer bytes = ByteableCollections.toByteBuffer(values);
        Path file = Paths.get(TestData.getTemporaryTestFile());
        String $file = file.toString();
        FileSystem.writeBytes(bytes, $file);
        int bufferSize = 8192;
        Benchmark benchmark1 = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                CloseableIterator<ByteBuffer> it = ByteableCollections.stream(file,
                        bufferSize);
                while (it.hasNext()) {
                    Value.fromByteBuffer(it.next());
                }
                it.closeQuietly();
            }

        };
        Benchmark benchmark2 = new Benchmark(TimeUnit.MILLISECONDS) {

            @SuppressWarnings("deprecation")
            @Override
            public void action() {
                Iterator<ByteBuffer> it = ByteableCollections
                        .streamingIterator($file, bufferSize);
                while (it.hasNext()) {
                    Value.fromByteBuffer(it.next());
                }
            }

        };
        AtomicDouble avg1 = new AtomicDouble();
        AtomicDouble avg2 = new AtomicDouble();
        CountUpLatch latch = new CountUpLatch();
        Thread t1 = new Thread(() -> {

            double avg = benchmark1.average(10);
            System.out.println("New: " + avg);
            avg1.set(avg);
            latch.countUp();

        });
        Thread t2 = new Thread(() -> {

            double avg = benchmark2.average(10);
            System.out.println("Deprecated: " + avg);
            avg2.set(avg);
            latch.countUp();

        });
        List<Thread> threads = Lists.newArrayList(t1, t2);
        Collections.shuffle(threads);
        for(Thread thread : threads) {
            thread.start();
        }
        latch.await(2);
        Assert.assertTrue(avg1.get() < avg2.get());
    }

}
