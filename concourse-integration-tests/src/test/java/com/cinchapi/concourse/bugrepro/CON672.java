/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.bugrepro;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit test to reproduce the issue of CON-672.
 *
 * @author Jeff Nelson
 */
public class CON672 extends ConcourseIntegrationTest {

    @Test
    public void reproTag() {
        Tag value = Tag.create("a=b");
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value(value);
        System.out.println(criteria.ccl());
        client.find(criteria);
        Assert.assertTrue(true); // lack of Exception means the test passes
    }

    @Test
    public void reproString() {
        String value = "a=b";
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value(value);
        System.out.println(criteria.ccl());
        client.find(criteria);
        Assert.assertTrue(true); // lack of Exception means the test passes
    }

}
