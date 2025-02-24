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
package com.cinchapi.concouse.ete.distributed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.automation.server.ManagedConcourseServer;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.ConcourseClusterTest;
import com.cinchapi.concourse.time.Time;

/**
 * Unit tests for basic operations in a distributed Concourse cluster.
 *
 * @author Jeff Nelson
 */
public class DistributedConcourseTest extends ConcourseClusterTest {

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
    public void testritesAcrossNodesReceiveSameTimestamp() {
        // This test verifies that Writes are stored on each holding node with
        // the same timestamp by using different coordinators to issue a Write
        // and then reviewing all those writes under the key to ensure that
        // there is no inconsistency in the returned values.
        List<Concourse> clients = new ArrayList<>();
        Iterator<ManagedConcourseServer> it = cluster.nodes().iterator();
        while (clients.size() < 2) {
            clients.add(it.next().connect());
        }
        for (Concourse client : clients) {
            client.add("name", "jeff" + Time.now(), 1);
        }
        Concourse client = cluster.connect();
        Map<Timestamp, List<String>> changes = client.review("name", 1);
        System.out.println(changes);
        Assert.assertEquals(2, changes.size());
    }

}
