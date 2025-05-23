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
package com.cinchapi.concourse.bugrepro;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableMap;

/**
 * Repro: https://github.com/cinchapi/concourse/issues/570
 *
 * @author Jeff Nelson
 */
public class GH570 extends ConcourseIntegrationTest {

    @Test
    public void testGH_570() {
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Map<String, Object> data = ImmutableMap.of("_", "Skill", "name",
                    Random.getSimpleString());
            client.insert(data);
        }
        Set<Long> actual = client.find(Criteria.where().key("_")
                .operator(Operator.LIKE).value("%Skill%"),
                Order.by("timestamp"));
        Set<Long> expected = client.find(Criteria.where().key("_")
                .operator(Operator.LIKE).value("%Skill%"), Order.by("name"));
        Assert.assertFalse(actual.isEmpty());
        Assert.assertEquals(expected, actual);
    }

}
