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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Resources;

/**
 * Unit tests to make sure that the select operation with
 * {@link Order} as a parameter work properly.
 */
public class SelectWithOrderTest extends ConcourseIntegrationTest {

    private Statement sql;

    @Override
    protected void beforeEachTest() {
        // Import data into Concourse
        System.out.println("Importing college data into Concourse");
        Importer importer = new CsvImporter(client);
        importer.importFile(Resources.get("/college.csv").getFile());

        // Load up the SQL db which also contains a copy of the data
        System.out.println("Loading SQL database with college data");
        try {
            // NOTE: The JDBC API is atrocious :o=
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection(
                    "jdbc:sqlite:" + Resources.get("/college.db").getFile());
            sql = conn.createStatement();
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }

        super.beforeEachTest();
    }

    @Test
    public void testWithOneOrderKey() {
        Criteria criteria = Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).build();
        Order order = Order.by("graduation_rate").build();

        List<Map<String, Object>> result =
                client.select(Lists.newArrayList("ipeds_id", "graduation_rate"), criteria,
                        order).values().stream().map(child ->
                        child.entrySet().stream().
                                collect(Collectors.toMap(Map.Entry::getKey,
                                        e -> Iterables.getOnlyElement(e.getValue())))
                ).collect(Collectors.toList());

        List<Map<String, Object>> expected =
                client.select(Lists.newArrayList("ipeds_id", "graduation_rate"), criteria)
                        .values().stream().map(child ->
                        child.entrySet().stream().
                                collect(Collectors.toMap(Map.Entry::getKey,
                                        e -> Iterables.getOnlyElement(e.getValue())))
                ).collect(Collectors.toList());

        expected.sort(Comparator.comparingInt(o -> (int) o.get("graduation_rate")));

        Assert.assertEquals(result, expected);
    }

    @Test
    public void testWithTwoOrderKeys() {
        Criteria criteria = Criteria.where().key("graduation_rate")
                .operator(Operator.GREATER_THAN).value(90).build();
        Order order = Order.by("graduation_rate").ascending()
                .then("city").ascending()
                .build();

        List<Map<String, Object>> result =
                client.select(Lists.newArrayList("ipeds_id", "graduation_rate", "city"), criteria,
                        order).values().stream().map(child ->
                            child.entrySet().stream().
                                    collect(Collectors.toMap(Map.Entry::getKey,
                                            e -> Iterables.getOnlyElement(e.getValue())))
                ).collect(Collectors.toList());

        List<Map<String, Object>> expected =
                client.select(Lists.newArrayList("ipeds_id", "graduation_rate", "city"), criteria)
                        .values().stream().map(child ->
                        child.entrySet().stream().
                                collect(Collectors.toMap(Map.Entry::getKey,
                                        e -> Iterables.getOnlyElement(e.getValue())))
                ).collect(Collectors.toList());

        expected.sort((o1, o2) -> {
                int c;
                c = ((Integer) o1.get("graduation_rate")).compareTo((Integer) o2.get("graduation_rate"));
                if (c == 0)
                    c = ((String) o1.get("city")).compareTo((String) o2.get("city"));
                return c;
            });

        Assert.assertEquals(result, expected);
    }
}
