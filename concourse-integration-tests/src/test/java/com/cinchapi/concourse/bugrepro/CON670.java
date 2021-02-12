/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * http://jira.cinchapi.com/browse/CON-670
 *
 * @author Jeff Nelson
 */
public class CON670 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        client.add("name", "jeffrey o");
        client.add("name", "jeffrey a");
        client.add("name", "jeff n");
        client.add("name", "jeffe c");
        client.add("name", "jefferson nel");
        Criteria condition = Criteria.where().key("name")
                .operator(Operator.LIKE).value("%jeff%");
        Set<Long> a = client.find(condition);
        Set<Long> b = client.select(condition).keySet();
        Iterator<Long> ait = a.iterator();
        Iterator<Long> bit = b.iterator();
        while (ait.hasNext()) {
            long anext = ait.next();
            long bnext = bit.next();
            Assert.assertEquals(anext, bnext);
        }
    }

}
