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
package com.cinchapi.concourse.bugrepro;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit test for repro of bug CON-473.
 * 
 * @author Jeff Nelson
 */
public class CON473 extends ConcourseIntegrationTest {

    @Test
    public void testRepro() {
        int target = 4000;
        client.set("count", 0, 1);
        for (int i = 0; i < target; ++i) {
            int incremented = i + 1;
            client.verifyAndSwap("count", i, 1, incremented);
        }
        Map<Timestamp, List<String>> review = client.review(1);
        int actual = 0;
        for (List<String> writes : review.values()) {
            actual += writes.size();
        }
        Assert.assertEquals((target * 2) + 1, actual);
    }

}
