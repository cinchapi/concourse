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
package com.cinchapi.concourse.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TLists;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link TLists}.
 * 
 * @author Jeff Nelson
 */
public class TListsTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testRetainIntersection() {
        List<Integer> common = Lists.newArrayList();
        List<Integer>[] lists = new List[Random.getScaleCount()];
        int max = Random.getScaleCount();
        for (int i = 0; i < max; i++) {
            common.add(i);
        }
        for (int i = 0; i < lists.length; i++) {
            lists[i] = Lists.<Integer> newArrayList();
            lists[i].addAll(common);
            for (int j = 0; j < Random.getScaleCount(); j++) {
                lists[i].add(Random.getScaleCount() + max);
            }
            lists[i] = Lists.newArrayList(lists[i]);
        }
        TLists.retainIntersection(lists);
        for (List<Integer> list : lists) {
            Assert.assertEquals(common, list);
        }

    }

}
