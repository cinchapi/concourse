/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

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
    public void testLessThanOrEqualsOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.LESS_THAN_OR_EQUALS, 2),
                client.find("age", "<=", 2));
    }

    @Test
    public void testGreaterThanOperatorConversion() {
        Assert.assertEquals(client.find("age", Operator.GREATER_THAN, 50),
                client.find("age", ">", 50));
    }

    @Test
    public void testGreaterThanOrEqualsOperatorToConversion() {
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

    // Testing CriteriaBuilder
    @Test
    public void testLessThanOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.LESS_THAN).value(25)),
                client.find(
                        Criteria.where().key("age").operator("<").value(25)));
    }

    @Test
    public void testLessThanOrEqualsOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.LESS_THAN_OR_EQUALS).value(2)),
                client.find(
                        Criteria.where().key("age").operator("<=").value(2)));
    }

    @Test
    public void testGreaterThanOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.GREATER_THAN).value(50)),
                client.find(
                        Criteria.where().key("age").operator(">").value(50)));
    }

    @Test
    public void testGreaterThanOrEqualsOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.GREATER_THAN_OR_EQUALS).value(99)),
                client.find(
                        Criteria.where().key("age").operator(">=").value(99)));
    }

    @Test
    public void testEqualsOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.EQUALS).value(15)),
                client.find(
                        Criteria.where().key("age").operator("=").value(15)));
    }

    @Test
    public void testNotEqualsOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.NOT_EQUALS).value(19)),
                client.find(
                        Criteria.where().key("age").operator("!=").value(19)));
    }

    @Test
    public void testBetweenOperatorConversionCb() {
        Assert.assertEquals(
                client.find(Criteria.where().key("age")
                        .operator(Operator.BETWEEN).value(40).value(50)),
                client.find(Criteria.where().key("age").operator("><").value(40)
                        .value(50)));
    }
}
