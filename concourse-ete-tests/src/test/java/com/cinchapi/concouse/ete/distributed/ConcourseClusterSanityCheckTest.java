/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concouse.ete.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.automation.server.ManagedConcourseServer;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.ConcourseClusterTest;
import com.google.common.collect.ImmutableMap;

/**
 *
 *
 * @author jeff
 */
public class ConcourseClusterSanityCheckTest extends ConcourseClusterTest {

    @Override
    public int clusterSize() {
        return 5;
    }

    @Override
    public int replicationFactor() {
        return 3;
    }

    @Override
    public String nodeVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testCanUseAnyCoordinator() {
        Concourse client = cluster.connect();
        long record = 1;
        System.out.println("Using client " + client);
        boolean added = client.add("name", "jeff", record);
        Assert.assertTrue(added);
        client = cluster.connect();
        List<Map<String, Set<Object>>> results = new ArrayList<>();
        for (ManagedConcourseServer node : cluster.nodes()) {
            client = node.connect();
            results.add(client.select(record));
        }
        Map<String, Set<Object>> last = null;
        for (Map<String, Set<Object>> result : results) {
            if(last == null) {
                last = result;
            }
            else {
                Assert.assertEquals(result, last);
            }
        }
    }
    
    @Test
    public void testCanUseAnyCoordinatorForAtomicOperation() {
        Concourse client = cluster.connect();
        System.out.println("Using client " + client);
        long record = client.insert(ImmutableMap.of("name", "jeff", "age", 35));
        System.out.println(record);
        client = cluster.connect();
        List<Map<String, Set<Object>>> results = new ArrayList<>();
        for (ManagedConcourseServer node : cluster.nodes()) {
            client = node.connect();
            results.add(client.select(record));
        }
        Map<String, Set<Object>> last = null;
        for (Map<String, Set<Object>> result : results) {
            if(last == null) {
                last = result;
            }
            else {
                Assert.assertEquals(result, last);
            }
        }
    }

}
