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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link com.cinchapi.concourse.Concourse#clear(long)} API
 * method. The clear method
 * takes any record and removes all the keys and the values contained inside the
 * key.
 *
 * @author hyin
 */
public class ClearTest extends ConcourseIntegrationTest {

    @Test
    public void testClear() {
        long record = TestData.getLong();
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.add("c", 1, record);
        client.add("c", 2, record);
        client.add("c", 3, record);
        client.add("d", 1, record);
        client.add("d", 2, record);
        client.add("d", 3, record);
        client.clear(record);
        Assert.assertTrue(client.select(record).isEmpty());
    }

    @Test
    public void testClearRecordList() {
        long record = 1;
        long record2 = 2;
        long record3 = 3;
        List<Long> recordsList = new ArrayList<Long>();
        recordsList.add(record);
        recordsList.add(record2);
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.add("c", 1, record2);
        client.add("c", 2, record2);
        client.add("c", 3, record2);
        client.add("d", 1, record3);
        client.add("d", 2, record3);
        client.add("d", 3, record3);
        client.clear(recordsList);
        Assert.assertEquals(client.select(record).isEmpty(),
                client.select(record2).isEmpty());
        Assert.assertFalse(client.select(record3).isEmpty());
    }

}
