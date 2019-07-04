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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.Resources;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;

/**
 * Unit tests to make sure that the select operation with {@link Page}
 * as a parameter works properly.
 * 
 * @author Javier Lores
 */
public class SelectWithPageTest extends ConcourseIntegrationTest {

    @Override
    protected void beforeEachTest() {
        // Import data into Concourse
        System.out.println("Start importing generated data into Concourse");
        Importer importer = new CsvImporter(client);
        importer.importFile(Resources.get("/generated.csv").getFile());
        System.out.println("Done importing generated data into Concourse");
        super.beforeEachTest();
    }

    @Test
    public void testFindKeyOperatorValuesPage() {
        String key = "dollar";
        Operator operator = Operator.GREATER_THAN;
        Integer value = 1000;
        Page page = Page.sized(10).go(2);
        Set<Long> result = client.find(key, operator, value, page);
        Assert.assertEquals(10, result.size());
    }

    @Test
    public void testSelectKeysCriteriaPage() {
        Criteria criteria = Criteria.where().key("dollar")
                .operator(Operator.GREATER_THAN).value(1000).build();
        Page page = Page.sized(10).go(2);
        Map<Long, Map<String, Set<Object>>> result = client
                .select(Lists.newArrayList("seq", "dollar"), criteria, page);
        Map<Long, Map<String, Set<Object>>> expected = client
                .select(Lists.newArrayList("seq", "dollar"), criteria);
        Assert.assertEquals(
                expected.values().stream().collect(Collectors.toList())
                        .subList(10, 20).stream().collect(Collectors.toList()),
                result.values().stream().collect(Collectors.toList()));
    }
}
