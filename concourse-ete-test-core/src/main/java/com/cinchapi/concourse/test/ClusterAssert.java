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
package com.cinchapi.concourse.test;

import java.util.function.Function;

import org.junit.Assert;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.automation.cluster.ManagedConcourseCluster;
import com.cinchapi.concourse.automation.server.ManagedConcourseServer;

/**
 * A set of assertions useful for writing tests that interact with
 * {@link ManagedConcourseCluster distributed clusters}.
 *
 * @author Jeff Nelson
 */
public class ClusterAssert {

    /**
     * Asserts that {@code function} returns an equal value for every node in
     * the {@code #cluster}.
     * 
     * @param function
     */
    public static <T> void assertConsensus(ManagedConcourseCluster cluster,
            Function<Concourse, T> function) {
        T expected = null;
        for (ManagedConcourseServer node : cluster.nodes()) {
            Concourse concourse = node.connect();
            T actual = function.apply(concourse);
            System.out.println(actual);
            if(expected != null) {
                Assert.assertEquals(AnyStrings.format(
                        "Divergent value on node {}. Expected {}, but got {}",
                        node, expected, actual), expected, actual);
            }
            expected = actual;
        }
    }

}
