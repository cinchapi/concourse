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
package com.cinchapi.concourse.server.storage.db.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

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
        Set<Text> ohts = OffHeapTextSet.test();
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
        Set<Text> ohts = OffHeapTextSet.test();
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

}