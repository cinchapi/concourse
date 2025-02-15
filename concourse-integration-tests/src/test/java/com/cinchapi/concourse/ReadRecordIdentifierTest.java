/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for querying on record identifiers.
 *
 * @author Jeff Nelson
 */
public class ReadRecordIdentifierTest extends ConcourseIntegrationTest {

    private final String key = Constants.JSON_RESERVED_IDENTIFIER_NAME;

    @Override
    public void beforeEachTest() {
        client.add("name", "Jeff Nelson", 1);
        client.add("name", "Ashleah Nelson", 2);
        client.add("name", "Reginald Moore", 3);
        client.add("name", "Aevyn Nelson", 4);
    }

    @Test
    public void testSelectionKey() {
        Set<Long> data = client.select(key, 1);
        Assert.assertEquals(ImmutableSet.of(1L), data);
    }

    @Test
    public void testSelectionKeyCcl() {
        Map<Long, Set<Long>> data = client.select(key, "name like %Nelson%");
        data.forEach((key, values) -> {
            Assert.assertEquals(ImmutableSet.of(key), values);
        });
    }

    @Test
    public void testNavigationSelectionKey() {
        client.link("spouse", 2, 1);
        client.link("spouse", 1, 2);
        Map<String, Set<Object>> data = client.select(
                ImmutableSet.of("name", "spouse.name", "spouse.$id$"), 1);
        Assert.assertEquals(ImmutableSet.of(2L), data.get("spouse.$id$"));
    }

    @Test
    public void testEvaluationKey() {
        Set<Long> data = client.find("name like %Nelson% and $id$ != 1");
        Assert.assertFalse(data.contains(1L));
    }

    @Test
    @Ignore // requires CCL bug fix
    public void testNavigationEvaluationKey() {
        Set<Long> data = client.find("name like %Nelson% and spouse.$id$ != 1");
        Assert.assertFalse(data.contains(2L));
    }

}
