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
package com.cinchapi.concouse.server.upgrade;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.test.Variables;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Integration test for {@link Upgrade0_5_0_2}.
 * 
 * @author Jeff Nelson
 */
public class UpgradeTask0_5_0_2Test extends UpgradeTest {

    private Set<Long> expected;

    @Override
    protected String getInitialServerVersion() {
        return "0.4.4";
    }

    @Override
    protected void preUpgradeActions() {
        int count = TestData.getScaleCount() * 3;
        List<Long> records = Lists.newArrayList();
        for (long i = 0; i < count; ++i) {
            if(i % 10 == 0) {
                continue;
            }
            else {
                records.add(i);
            }
        }
        Collections.shuffle(records);
        for (long record : records) {
            client.add("foo", record, record);
            if(TestData.getInt() % 3 == 0) {
                client.remove("foo", record, record);
            }
        }
        expected = Variables.register("expected", Sets.newHashSet(records));
    }

    @Test
    public void testInventoryIsPopulated() {
        Set<Long> actual = client.inventory();
        Variables.register("actual", actual);
        Assert.assertEquals(expected, actual);
    }

}
