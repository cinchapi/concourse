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
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

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
        String key = "graduation_rate" ;
        Operator operator = Operator.GREATER_THAN;
        Integer value = 90;
        Page page = Page.with(10).to(2);

        Set<Long> result = client.find(key, operator, value, page);

        Assert.assertEquals(10, result.size());
    }

    @Test
    public void testSelectKeysCriteriaPage() {
        Criteria criteria = Criteria.where().key("graduation_rate")
                .operator(Operator.GREATER_THAN).value(90).build();
        Page page = Page.with(10).to(2);

        long startTime = System.nanoTime();
        client.select(Lists.newArrayList("ipeds_id", "graduation_rate"),
                criteria, page);
        long endTime = System.nanoTime();

        long duration = (endTime - startTime) / 1000000;
        System.out.println(duration);

        List<Map<String, Object>> result = client
                .select(Lists.newArrayList("ipeds_id", "graduation_rate"),
                        criteria, page)
                .values().stream()
                .map(child -> child.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Iterables.getOnlyElement(e.getValue()))))
                .collect(Collectors.toList());

        List<Map<String, Object>> expected = client
                .select(Lists.newArrayList("ipeds_id", "graduation_rate"),
                        criteria)
                .values().stream()
                .map(child -> child.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Iterables.getOnlyElement(e.getValue()))))
                .collect(Collectors.toList());

        Assert.assertEquals(expected.subList(10, 20), result);
    }

    @Test
    public void testPerformance() {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        Page page = Page.with(10000).to(1);

        int numTestRuns = 100;
        int total = 0;
        for(int i = 0; i < numTestRuns; i++) {
            long startTime = System.nanoTime();
            client.select(Lists.newArrayList("seq", "name"),
                    criteria);
            long endTime = System.nanoTime();

            System.out.println((endTime - startTime) / 1000000);
            if(i > 0) {
                total += (endTime - startTime) / 1000000;
            }
        }

        long duration = total / (numTestRuns - 1);
        System.out.println("average time: " + duration);

    }
}
