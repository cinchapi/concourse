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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Lists;

/**
 * Unit tests for result set pagination functionality
 *
 * @author Jeff Nelson
 */
public class ResultPaginationTest extends ConcourseIntegrationTest {

    @Test
    public void testSelectRecordsPage() {
        List<Long> records = Lists.newArrayList();
        for (long i = 0; i < 100; ++i) {
            client.add("foo", i, i);
            records.add(i);
        }
        List<Long> $records = Lists.newArrayList(records);
        Page page = Page.sized(15);
        Map<Long, Map<String, Set<Object>>> data = null;
        while (data == null || !data.isEmpty()) {
            data = client.select($records, page);
            for (long record : data.keySet()) {
                Assert.assertTrue(record >= page.skip()
                        && record <= (page.limit() + page.skip()));
            }
            records.removeAll(data.keySet());
            page = page.next();
        }
        Assert.assertTrue(records.isEmpty());

    }

}
