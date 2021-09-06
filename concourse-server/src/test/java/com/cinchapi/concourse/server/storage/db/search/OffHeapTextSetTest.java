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
package com.cinchapi.concourse.server.storage.db.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link OffHeapTextSet}.
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("resource")
public class OffHeapTextSetTest {

    @Test
    public void testContains() {
        List<String> corpus = new ArrayList<>();
        for (int i = 0; i < 5 * TestData.getScaleCount(); ++i) {
            corpus.add(Random.getString());
        }
        Set<Text> ohts = new OffHeapTextSet();
        Set<Text> control = new HashSet<>();
        Iterator<String> it = corpus.iterator();
        while (it.hasNext()) {
            Text string = Text.wrap(it.next());
            if(TestData.getScaleCount() % 3 == 0) {
                control.add(string);
                ohts.add(string);
            }
        }
        it = corpus.iterator();
        while (it.hasNext()) {
            Text string = Text.wrap(it.next());
            Assert.assertEquals(control.contains(string),
                    ohts.contains(string));
        }
    }

    @Test
    public void testAdd() {
        List<String> corpus = new ArrayList<>();
        for (int i = 0; i < 5 * TestData.getScaleCount(); ++i) {
            corpus.add(Random.getString());
        }
        Set<Text> ohts = new OffHeapTextSet();
        Set<Text> control = new HashSet<>();
        Iterator<String> it = corpus.iterator();
        while (it.hasNext()) {
            Text string = Text.wrap(it.next());
            if(TestData.getScaleCount() % 3 == 0) {
                control.add(string);
                ohts.add(string);
            }
        }
        it = corpus.iterator();
        while (it.hasNext()) {
            Text string = Text.wrap(it.next());
            if(TestData.getScaleCount() % 2 == 0) {
                Assert.assertEquals(control.add(string), ohts.add(string));
            }

        }
    }

    @Test
    public void testAddAllSubstrings() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1; ++i) {
            sb.append(TestData.getString());
        }
        String string = sb.toString();
        Set<String> infixes = AnyStrings.getAllSubStrings(string);
        System.out.println(infixes.size());
        Benchmark b1 = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                Set<Text> set = new HashSet<>();
                for (String string : infixes) {
                    Text text = Text.wrap(string);
                    set.add(text);
                }
            }

        };
        Benchmark b2 = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                Set<Text> set = new OffHeapTextSet();
                for (String string : infixes) {
                    Text text = Text.wrap(string);
                    set.add(text);
                }
            }

        };
        double avg1 = b1.average(1);
        double avg2 = b2.average(1);
        System.out.println(AnyStrings.format(
                "HashSet took {} ms and OffHeapTextSet took {} ms", avg1,
                avg2));
    }

}
