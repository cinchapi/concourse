/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests to validate performing searches using the {@link Operator#CONTAINS
 * CONTAINS} and {@link Operator#NOT_CONTAINS NOT_CONTAINS} operators.
 *
 * @author Jeff Nelson
 */
public class FindSearchTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        long jeff = client
                .insert(ImmutableMap.of("name", "Jeff Nelson", "age", 36));
        long ashleah = client
                .insert(ImmutableMap.of("name", "Ashleah Nelson", "age", 36));
        long reniya = client
                .insert(ImmutableMap.of("name", "Reniya Davis", "age", 25));
        long cinchapi = client.insert(ImmutableMap.of("name", "Cinchapi"));
        long blavity = client.insert(ImmutableMap.of("name", "Blavity"));
        client.link("company", ImmutableList.of(cinchapi, blavity), jeff);
        client.link("company", blavity, reniya);
        client.add("interests", "Blavity", ashleah);
    }

    @Test
    public void testKeyContainsTrue() {
        String key = "name";
        String search = "els";
        Set<Long> records = client.find(key, Operator.CONTAINS, search);
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testKeyContainsFalse() {
        String key = "name";
        String search = "elss";
        Set<Long> records = client.find(key, Operator.CONTAINS, search);
        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testKeyNotContainsTrue() {
        String key = "name";
        String search = "els";
        Set<Long> records = client.find(key, Operator.NOT_CONTAINS, search);
        Assert.assertEquals(3, records.size());
    }

    @Test
    public void testKeyNotContainsFalse() {
        String key = "name";
        String search = "elss";
        Set<Long> records = client.find(key, Operator.NOT_CONTAINS, search);
        Assert.assertEquals(5, records.size());
    }

    @Test
    public void testNavigationContainsTrue() {
        String key = "company.name";
        String search = "lavi";
        Set<Long> records = client.find(key, Operator.CONTAINS, search);
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testNavigationContainsFalse() {
        String key = "company.name";
        String search = "Blank";
        Set<Long> records = client.find(key, Operator.CONTAINS, search);
        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testNavigationNotContainsTrue() {
        String key = "company.name";
        String search = "lavi";
        Set<Long> records = client.find(key, Operator.NOT_CONTAINS, search);
        Assert.assertEquals(1, records.size());
    }

    @Test
    public void testNavigationNotContainsFalse() {
        String key = "company.name";
        String search = "a";
        Set<Long> records = client.find(key, Operator.NOT_CONTAINS, search);
        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testContainsNonStringValues() {
        String key = "company";
        String search = "Blav";
        Assert.assertEquals(client.search(key, search),
                client.find(key, Operator.CONTAINS, search));
    }

    @Test
    public void testNavigationContainsPerformance()
            throws InterruptedException {
        String key = "name";
        String search = "lavi";
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            if(TestData.getInt() % 3 == 0) {
                client.add("name", search);
            }
            else {
                client.add("name", TestData.getString());
            }
        }
        Benchmark regex = new Benchmark(TimeUnit.MILLISECONDS) {
            String query = AnyStrings.format("'(?i)%{}%'", search);
            Concourse c = Concourse.copyExistingConnection(client);

            @Override
            public void action() {
                c.find(key, Operator.LIKE, query);
            }

        };

        Benchmark query = new Benchmark(TimeUnit.MILLISECONDS) {
            Concourse c = Concourse.copyExistingConnection(client);

            @Override
            public void action() {
                c.find(key, Operator.CONTAINS, search);

            }

        };
        Thread t1 = new Thread(() -> {
            double q = query.average(5);
            System.out.println("search = " + q + " ms");
        });
        Thread t2 = new Thread(() -> {
            double r = regex.average(5);
            System.out.println("regex = " + r + " ms");
        });
        t1.start();
        t2.start();
        t2.join();
        t1.join();

    }

    /*
     * - something with time
     */

}
