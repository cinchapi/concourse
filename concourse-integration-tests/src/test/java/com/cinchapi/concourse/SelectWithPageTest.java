package com.cinchapi.concourse;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.pagination.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * Unit tests to make sure that the select operation with {@link Page}
 * as a parameter works properly.
 */
public class SelectWithPageTest extends ConcourseIntegrationTest {

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
    public void testWithPage() {
        Assert.assertTrue(hasSameResults(Criteria.where().key("graduation_rate")
                        .operator(Operator.GREATER_THAN).value(90).and()
                        .key("yield_men").operator(Operator.EQUALS).value(20).build(),
                Page.with(10).to(5)));
    }

    /**
     * Validate that the {@code criteria} and {@code page} returns the same
     * result in Concourse as it does in a relational database.
     *
     * @param page
     * @return {@code true} if the Concourse and SQL result sets are the same
     */
    private boolean hasSameResults(Criteria criteria, Page page) {
        try {
            Set<Object> a = Sets.newHashSet(
                    client.get("ipeds_id", client.find(criteria)).values());
            String query = "SELECT ipeds_id FROM data WHERE " + criteria.toString()
                            + " LIMIT " + page.limit() + " OFFSET " + page.skip();
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
