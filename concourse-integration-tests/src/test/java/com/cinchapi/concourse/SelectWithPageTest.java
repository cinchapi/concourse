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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.pagination.Page;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Unit tests to make sure that the select operation with {@link Page}
 * as a parameter works properly.
 */
public class SelectWithPageTest extends ConcourseIntegrationTest {

    @Override
    protected void beforeEachTest() {
        // Import data into Concourse
        System.out.println("Importing college data into Concourse");
        Importer importer = new CsvImporter(client);
        importer.importFile(Resources.get("/generated.csv").getFile());

        super.beforeEachTest();
    }

    @Test
    public void testFindKeyOperatorValuesPage() {
        String key = "dollar";
        Operator operator = Operator.GREATER_THAN;
        Integer value = 1000;
        Page page = Page.with(10).to(2);

        Set<Long> result = client.find(key, operator, value, page);

        Assert.assertEquals(10, result.size());
    }

    @Test
    public void testSelectKeysCriteriaPage() {
        Criteria criteria = Criteria.where().key("dollar")
                .operator(Operator.GREATER_THAN).value(1000).build();
        Page page = Page.with(10).to(2);

        client.select(Lists.newArrayList("seq", "dollar"), criteria, page);

        List<Map<String, Object>> result = client
                .select(Lists.newArrayList("seq", "dollar"), criteria, page)
                .values().stream()
                .map(child -> child.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Iterables.getOnlyElement(e.getValue()))))
                .collect(Collectors.toList());

        List<Map<String, Object>> expected = client
                .select(Lists.newArrayList("seq", "dollar"), criteria).values()
                .stream()
                .map(child -> child.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Iterables.getOnlyElement(e.getValue()))))
                .collect(Collectors.toList());

        Assert.assertEquals(expected.subList(10, 20), result);
    }

    @Test
    public void testPerformance() throws InterruptedException {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        Page page = Page.with(10000).to(1);
        TimeUnit unit = TimeUnit.MILLISECONDS;

        Benchmark bench = new Benchmark(unit) {
            @Override
            public void action() {
                client.select(Lists.newArrayList("seq", "name"), criteria,
                        page);
            }
        };

        int rounds = 100;
        // Warm up the call
        client.select(Lists.newArrayList("seq", "name"), criteria, page);
        long time = 0;
        for (int i = 0; i < rounds; i++) {
            long runTime = bench.run();
            time += runTime;
        }
        time = time / rounds;

        System.out.println("Average runtime: " + time);
        Assert.assertTrue(time <= 7000);

    }
}
