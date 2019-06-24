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
package com.cinchapi.concourse;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Order ordering} results.
 *
 * @author Jeff Nelson
 */
public class ResultOrderTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        seed();
    }

    @Test
    public void testSelectCclNoOrder() {
        Map<Long, Map<String, Set<Object>>> data = client
                .select("active = true");
        long last = 0;
        for (long record : data.keySet()) {
            Assert.assertTrue(record > last);
            last = record;
        }
    }

    @Test
    public void testSelectCclOrder() {
        Map<Long, Map<String, Set<Object>>> data = client.select(
                "active = true", Order.by("score").then("name").largestFirst());
        String name = "";
        int score = 0;
        for (Map<String, Set<Object>> row : data.values()) {
            String $name = (String) row.get("name").iterator().next();
            int $score = (int) row.get("score").iterator().next();
            Assert.assertTrue(Integer.compare($score, score) > 0
                    || (Integer.compare($score, score) == 0
                            && $name.compareTo(name) < 0));
            name = $name;
            score = $score;
        }
    }

    /**
     * Seed the test database.
     */
    private void seed() {
        long a = client.insert(ImmutableMap.of("name", "Jeff Nelson", "age", 31,
                "company", "Cinchapi", "active", true, "score", 100));
        long b = client.insert(ImmutableMap.of("name", "John Doe", "age", 40,
                "company", "Blavity", "active", false, "score", 95));
        long c = client.insert(ImmutableMap.of("name", "Jeff Johnson", "age",
                38, "company", "BET", "active", true, "score", 99));
        long d = client.insert(ImmutableMap.of("name", "Ashleah Nelson", "age",
                31, "company", "Cinchapi", "active", false, "score", 100));
        long e = client.insert(ImmutableMap.of("name", "Barack Obama", "age",
                56, "company", "United States", "active", false, "score", 80));
        long f = client.insert(ImmutableMap.of("name", "Ronald Reagan", "age",
                89, "company", "United States", "active", false, "score", 80));
        long g = client.insert(ImmutableMap.of("name", "Zane Miller", "age", 31,
                "company", "Cinchapi", "active", true, "score", 100));
        long h = client.insert(ImmutableMap.of("name", "Peter Griffin", "age",
                40, "company", "Family Guy", "active", true, "score", 23));
        client.link("friends", ImmutableList.of(b, c, f, h), a);
        client.link("friends", ImmutableList.of(a, d, g), b);
        client.link("friends", ImmutableList.of(e), c);
        client.link("friends", ImmutableList.of(a, b, e, g, h), d);
        client.link("friends", ImmutableList.of(a), e);
        client.link("friends", ImmutableList.of(a, b, e, g, h), f);
        client.link("friends", ImmutableList.of(c, d, h), g);
        client.link("friends", ImmutableList.of(a, b, f), h);
    }

}
