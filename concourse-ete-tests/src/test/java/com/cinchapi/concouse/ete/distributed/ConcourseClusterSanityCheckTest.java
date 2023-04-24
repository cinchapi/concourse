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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.automation.server.ManagedConcourseServer;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.ConcourseClusterTest;

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
        long record = client.add("name", "jeff");
        client = cluster.connect();
        for (ManagedConcourseServer node : cluster.nodes()) {
            client = node.connect();
            System.out.println(client.select(record));
        }
        Assert.assertFalse(true);
        /*
         * TODO:
         * - only 1 node is acking the write when at least 3 should
         * - the same coordinator cohort should be chose so each node should
         * print the right thing even if its only acked by 1
         */
    }

}
