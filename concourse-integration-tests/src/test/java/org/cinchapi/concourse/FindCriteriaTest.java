/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import org.cinchapi.concourse.importer.GeneralCsvImporter;
import org.cinchapi.concourse.importer.Importer;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Resources;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Unit tests to make sure that the find operation works with complex
 * {@link Criteria}.
 * 
 * @author jnelson
 */
public class FindCriteriaTest extends ConcourseIntegrationTest {

    private Statement sql;

    @Override
    protected void beforeEachTest() {
        // Import data into Concourse
        System.out.println("Importing college data into Concourse");
        Importer importer = GeneralCsvImporter.withConnectionInfo(SERVER_HOST,
                SERVER_PORT, "admin", "admin");
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
    public void testSimple() {
        Assert.assertTrue(hasSameResults(Criteria.builder()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).build()));
    }

    @Test
    public void testSimpleAnd() {
        Assert.assertTrue(hasSameResults(Criteria.builder()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).and().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).build()));
    }

    @Test
    public void testSimpleOr() {
        Assert.assertTrue(hasSameResults(Criteria.builder()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).or().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).build()));
    }

    @Test
    public void testSimpleAndOr() {
        Assert.assertTrue(hasSameResults(Criteria.builder()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).and().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).or()
                .key("total_cost_out_state").operator(Operator.GREATER_THAN)
                .value(50000).build()));
    }

    @Test
    public void testSimpleOrAnd() {
        Assert.assertTrue(hasSameResults(Criteria.builder()
                .key("graduation_rate").operator(Operator.GREATER_THAN)
                .value(90).or().key("percent_undergrad_black")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(5).and()
                .key("total_cost_out_state").operator(Operator.GREATER_THAN)
                .value(50000).build()));
    }

    @Test
    public void testAndGroupOr() {
        Assert.assertTrue(hasSameResults(Criteria
                .builder()
                .key("graduation_rate")
                .operator(Operator.GREATER_THAN)
                .value(90)
                .and()
                .group(Criteria.builder().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .or().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testOrGroupAnd() {
        Assert.assertTrue(hasSameResults(Criteria
                .builder()
                .key("graduation_rate")
                .operator(Operator.GREATER_THAN)
                .value(90)
                .or()
                .group(Criteria.builder().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .and().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testGroupAndOrGroupAnd() {
        Assert.assertTrue(hasSameResults(Criteria
                .builder()
                .group(Criteria.builder().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).and()
                        .key("yield_men").operator(Operator.EQUALS).value(20)
                        .build())
                .or()
                .group(Criteria.builder().key("percent_undergrad_black")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(5)
                        .and().key("total_cost_out_state")
                        .operator(Operator.GREATER_THAN).value(50000).build())
                .build()));
    }

    @Test
    public void testGroupOrAndGroupOr() {
        Assert.assertTrue(hasSameResults(Criteria
                .builder()
                .group(Criteria.builder().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).or()
                        .key("yield_men").operator(Operator.EQUALS).value(20)
                        .build())
                .and()
                .group(Criteria.builder().key("percent_undergrad_black")
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
