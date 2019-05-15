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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.Sets;

/**
 *
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
        Assert.assertTrue(hasSameResults(Criteria.where().key("graduation_rate")
                .operator(Operator.GREATER_THAN).value(90).and()
                .key("yield_men").operator(Operator.EQUALS).value(20).build(),
                Order.by("graduation_rate").ascending()
                        .then("undergraduate_population").descending()
                        .build()));
    }

    @Test
    public void testWithTwoOrderKeys() {
        Assert.assertTrue(hasSameResults(Criteria.where().key("graduation_rate")
                .operator(Operator.GREATER_THAN).value(90).and()
                .key("yield_men").operator(Operator.EQUALS).value(20).build(),
                Order.by("graduation_rate").build()));
    }

    /**
     * Validate that the {@code criteria} and {@code order} returns the same
     * result in Concourse as it does in a relational database.
     *
     * @param order
     * @return {@code true} if the Concourse and SQL result sets are the same
     */
    private boolean hasSameResults(Criteria criteria, Order order) {
        try {
            Set<Object> a = Sets.newHashSet(
                    client.get("ipeds_id", client.find(criteria)).values());
            String query = "SELECT ipeds_id FROM data WHERE "
                    + criteria.toString() + " ORDER BY " + order.getSQLString();
            ResultSet rs = sql.executeQuery(query);
            rs.next(); // skip column header
            Set<Object> b = Sets.newHashSet();
            while (rs.next()) {
                b.add(rs.getInt(1));
            }
            Variables.register("query", query);
            Variables.register("con", a);
            Variables.register("sql", b);
            Variables.register("diff", Sets.symmetricDifference(a, b));
            return a.equals(b);

        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }
}
