/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Resources;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Unit tests to make sure that the find operation works with complex
 * {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
public class FindCriteriaTest extends ConcourseIntegrationTest {

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
            Connection conn = DriverManager.getConnection("jdbc:sqlite:"
                    + Resources.get("/college.db").getFile());
            sql = conn.createStatement();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        super.beforeEachTest();
    }

    @Test
    public void testSimpleWithTime() {
        Set<Long> results = client.find(Criteria.where().key("graduation_rate")
                .operator(Operator.GREATER_THAN).value(90));
        Timestamp t1 = Timestamp.now();
        System.out.println("Importing college data into Concourse");
        Importer importer = new CsvImporter(client);
        importer.importFile(Resources.get("/college.csv").getFile());
        Assert.assertEquals(
                results,
                client.find(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).at(t1)));

    }

    @Test
    public void testBuildableStateParamSucceeds() {
        Assert.assertEquals(
                client.find(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90)),
                client.find(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonBuildableStateParamDoesNotSucceed() {
        client.find(new Object());
    }

    @Test
    public void testSimple() {
        Assert.assertTrue(hasSameResults(Criteria.where()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).build()));
    }

    @Test
    public void testSimpleAnd() {
        Assert.assertTrue(hasSameResults(Criteria.where()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).and().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).build()));
    }

    @Test
    public void testSimpleOr() {
        Assert.assertTrue(hasSameResults(Criteria.where()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).or().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).build()));
    }

    @Test
    public void testSimpleAndOr() {
        Assert.assertTrue(hasSameResults(Criteria.where()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).and().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).or()
                .key("total_cost_out_state").operator(Operator.GREATER_THAN)
                .value(50000).build()));
    }

    @Test
    public void testSimpleOrAnd() {
        Assert.assertTrue(hasSameResults(Criteria.where()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).or().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).and()
                .key("total_cost_out_state").operator(Operator.GREATER_THAN)
                .value(50000).build()));
    }

    @Test
    public void testAndGroupOr() {
        Assert.assertTrue(hasSameResults(Criteria
                .where()
                .key("graduation_rate")
                .operator(Operator.GREATER_THAN)
                .value(90)
                .and()
                .group(Criteria.where().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .or().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testOrGroupAnd() {
        Assert.assertTrue(hasSameResults(Criteria
                .where()
                .key("graduation_rate")
                .operator(Operator.GREATER_THAN)
                .value(90)
                .or()
                .group(Criteria.where().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .and().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testGroupAndOrGroupAnd() {
        Assert.assertTrue(hasSameResults(Criteria
                .where()
                .group(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).and()
                        .key("yield_men").operator(Operator.EQUALS).value(20)
                        .build())
                .or()
                .group(Criteria.where().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .and().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testGroupOrAndGroupOr() {
        Assert.assertTrue(hasSameResults(Criteria
                .where()
                .group(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).or()
                        .key("yield_men").operator(Operator.EQUALS).value(20)
                        .build())
                .and()
                .group(Criteria.where().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .or().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    /**
     * Validate that the {@code criteria} returns the same result in Concourse
     * as it does in a relational database.
     * 
     * @param criteria
     * @return {@code true} if the Concourse and SQL result sets are the same
     */
    private boolean hasSameResults(Criteria criteria) {
        try {
            Set<Object> a = Sets.newHashSet(client.get("ipeds_id",
                    client.find(criteria)).values());
            String query = "SELECT ipeds_id FROM data WHERE "
                    + criteria.toString();
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
            throw Throwables.propagate(e);
        }
    }

}
