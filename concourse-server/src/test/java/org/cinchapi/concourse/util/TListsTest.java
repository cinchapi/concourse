/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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
package org.cinchapi.concourse.util;

import java.util.List;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.testing.Variables;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link TLists}.
 * 
 * @author Jeff Nelson
 */
public class TListsTest extends ConcourseBaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testRetainIntersection() {
        List<Integer> common = Variables.register("common",
                Lists.<Integer> newArrayList());
        List<Integer>[] lists = new List[TestData.getScaleCount()];
        int max = TestData.getScaleCount();
        for (int i = 0; i < max; i++) {
            common.add(i);
        }
        for (int i = 0; i < lists.length; i++) {
            lists[i] = Variables.register("list_" + i,
                    Lists.<Integer> newArrayList());
            lists[i].addAll(common);
            for (int j = 0; j < TestData.getScaleCount(); j++) {
                lists[i].add(TestData.getScaleCount() + max);
            }
            lists[i] = Lists.newArrayList(lists[i]);
        }
        TLists.retainIntersection(lists);
        for (List<Integer> list : lists) {
            Assert.assertEquals(common, list);
        }

    }

}
