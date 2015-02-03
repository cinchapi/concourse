package org.cinchapi.concourse;

import org.cinchapi.concourse.thrift.Operator;

import org.junit.Assert;
import org.junit.Test;

public class FindOperatorConversionTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        client.add("name", "Johnny Doe1", 1);
        client.add("age", 19, 1);
        client.add("name", "Johnny Doe2", 2);
        client.add("age", 23, 2);
        client.add("name", "Johnny Doe3", 3);
        client.add("age", 2, 3);
        client.add("name", "Johnny Doe4", 4);
        client.add("age", 1, 4);
        client.add("name", "Johnny Doe5", 5);
        client.add("age", 21, 5);
        client.add("name", "Johnny Doe6", 6);
        client.add("age", 26, 6);
        client.add("name", "Johnny Doe7", 7);
        client.add("age", 48, 7);
        client.add("name", "Johnny Doe8", 8);
        client.add("age", 99, 8);
        client.add("name", "Johnny Doe9", 9);
        client.add("age", 15, 9);
        client.add("name", "Johnny Doe10", 10);
        client.add("age", 44, 10);
    }

    @Test
    public void testLessThanOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.LESS_THAN, 25),
                client.find("age", "<", 25));
    }

    @Test
    public void testLessThanOrEqualToOperatorConversion() {
        Assert.assertEquals(
                client.find("age", Operator.LESS_THAN_OR_EQUALS, 2),
                client.find("age", "<=", 2));
    }

    @Test
    public void testGreaterThanOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.GREATER_THAN, 50),
                client.find("age", ">", 50));
    }

    @Test
    public void testGreaterThanOrEqualOperatorToConversion() {
        Assert.assertEquals(
                client.find("age", Operator.GREATER_THAN_OR_EQUALS, 99),
                client.find("age", ">=", 99));
    }

    @Test
    public void testEqualsOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.EQUALS, 15),
                client.find("age", "=", 15));
    }

    @Test
    public void testNotEqualsOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.NOT_EQUALS, 19),
                client.find("age", "!=", 19));
    }

    @Test
    public void testBetweenOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.BETWEEN, 40, 50),
                client.find("age", "><", 40, 50));
    }
}
