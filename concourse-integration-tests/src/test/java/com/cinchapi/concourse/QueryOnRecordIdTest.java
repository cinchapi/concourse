/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Sets;

/**
 * Class includes test cases for a feature in findAtomic() method in
 * ConcourseServer class.
 * It includes test for handling id related expression in query at the Server
 * rather than passing the query to Engine.
 * 
 * @author Raghav
 */
public class QueryOnRecordIdTest extends ConcourseIntegrationTest {

    @Override
    protected void beforeEachTest() {
        for (int i = 30; i <= 50; i++) {
            client.add("name", "foo" + i, i);
        }
    }

    @Test
    public void testRecordRetrievalMatchingId() {
        Set<Long> set = Sets.newHashSet();
        set.add(new Long(35));
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(35).build()));
    }

    @Test
    public void testRecordRetrievalNotMatchingId() {
        Set<Long> set = Sets.newHashSet();
        for (long i = 30; i <= 50; i++) {
            if(i != 35) {
                set.add(i);
            }
        }
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.NOT_EQUALS).value(35).build()));
    }

    @Test
    public void testRecordRetrievaIWithIdAndOperator() {
        Set<Long> set = Sets.newHashSet();
        set.add(new Long(35));
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(35).and().key("name")
                        .operator(Operator.EQUALS).value("foo35").build()));
        set = Sets.newHashSet();
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(55).and().key("name")
                        .operator(Operator.EQUALS).value("foo35").build()));
    }

    @Test
    public void testRecordRetrievaIWithIdOrOperator() {
        Set<Long> set = Sets.newHashSet();
        set.add(new Long(35));
        set.add(new Long(40));
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(35).or().key("name")
                        .operator(Operator.EQUALS).value("foo40").build()));

        set = Sets.newHashSet();
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(55).and().key("name")
                        .operator(Operator.EQUALS).value("foo46").build()));
    }

    @Test
    public void testRecordRetrievalNonExistingId() {
        Set<Long> set = Sets.newHashSet();
        set.add(new Long(55));
        Assert.assertEquals(
                set,
                client.find(Criteria.where()
                        .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                        .operator(Operator.EQUALS).value(55).build()));
    }

    @Test(expected = Exception.class)
    public void testQueryOnRecordIdNonEqualOrNonEqualsOperator() {
        Operator operator = Operator.GREATER_THAN;
        client.find(Criteria.where()
                .key(Constants.JSON_RESERVED_IDENTIFIER_NAME)
                .operator(operator).value(55).build());
    }
    
    @Test
    public void testQueryOnRecordIdComplexCcl(){
        for (int i = 30; i <= 50; i++) {
            client.add("name", "foo", i);
        }
        for(int i = 0; i < 20; ++i){
            client.add("bar", i, i);
        }
        String ccl = "(name = foo OR bar > 15) AND $id$ != 40";
        Set<Long> actual = client.find(ccl);
        Assert.assertFalse(actual.contains(40));
    }

}
