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
package com.cinchapi.concouse.server.upgrade;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.ClientServerTests;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;

/**
 * Upgrade task to ensure that data in Blocks is properly transferred to segment
 * files.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_11_0_1Test extends UpgradeTest {

    @Override
    protected String getInitialServerVersion() {
        return "0.10.6";
    }

    private String[] envs;
    Map<String, Consumer<Concourse>> tests = Maps.newHashMap();

    @Override
    protected void preUpgradeActions() {
        envs = new String[] { "foo", "bar" };
        ClientServerTests.insertRandomData(server, envs);
        for (String env : envs) {
            Concourse concourse = server.connect("admin", "admin", env);
            try {
                Set<Long> inventory = concourse.inventory();
                Map<Long, Map<String, Set<Object>>> records = Maps.newHashMap();
                for (long record : inventory) {
                    if(TestData.getScaleCount() % 3 == 0) {
                        records.put(record, concourse.select(record));
                    }
                }
                Map<String, Map<Object, Set<Long>>> indexes = Maps
                        .newLinkedHashMap();
                Set<String> keys = concourse.describe();
                for (String key : concourse.describe()) {
                    if(TestData.getScaleCount() % 3 == 0) {
                        indexes.put(key, concourse.browse(key));
                    }
                }
                tests.put(env, con -> {
                    System.out.println("Checking inventory...");
                    Assert.assertEquals(inventory, con.inventory());
                    System.out.println("Checking random records...");
                    for (Entry<Long, Map<String, Set<Object>>> entry : records
                            .entrySet()) {
                        Map<String, Set<Object>> expected = entry.getValue();
                        long record = entry.getKey();
                        Assert.assertEquals(expected, con.select(record));
                    }
                    System.out.println("Checking dictionary...");
                    Assert.assertEquals(keys, con.describe());
                    System.out.println("Checking random indexes...");
                    for (Entry<String, Map<Object, Set<Long>>> entry : indexes
                            .entrySet()) {
                        Map<Object, Set<Long>> expected = entry.getValue();
                        String key = entry.getKey();
                        Assert.assertEquals(expected, con.browse(key));
                    }
                });
            }
            finally {
                concourse.close();
            }
        }
    }

    @Test
    public void testTransferBlockDataToSegments() {
        for (String env : envs) {
            Concourse concourse = server.connect("admin", "admin", env);
            try {
                tests.get(env).accept(concourse);
            }
            finally {
                concourse.close();
            }
        }
    }

}
