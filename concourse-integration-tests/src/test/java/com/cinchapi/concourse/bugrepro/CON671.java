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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableMap;

/**
 * Bug repro for CON-671
 *
 * @author Jeff Nelson
 */
public class CON671 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        long a = client.insert(ImmutableMap.of("name", "Albert", "order", 1));
        long d = client.insert(ImmutableMap.of("name", "David", "order", 4));
        long b = client.insert(ImmutableMap.of("name", "Bob", "order", 2));
        long c = client.insert(ImmutableMap.of("name", "Chris", "order", 3));

        client.link("partner", c, a);
        client.link("partner", a, c);
        client.link("partner", d, b);
        client.link("partner", b, d);
        Set<Long> expected = client
                .select("partner.name", "order < 5", Order.by("partner.name"))
                .keySet();
        Set<Long> actual = client
                .select("order", "order < 5", Order.by("partner.name"))
                .keySet();
        Assert.assertEquals(expected.toString(), actual.toString()); // Comparing
                                                                     // toString
                                                                     // is more
                                                                     // convenient

    }

}
