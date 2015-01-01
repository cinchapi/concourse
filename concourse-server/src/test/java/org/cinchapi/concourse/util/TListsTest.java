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
 * @author jnelson
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
