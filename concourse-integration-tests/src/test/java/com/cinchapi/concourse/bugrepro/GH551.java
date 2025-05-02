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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableSet;

/**
 * Unit test for https://github.com/cinchapi/concourse/issues/551
 *
 * @author Jeff Nelson
 */
public class GH551 extends ConcourseIntegrationTest {

    @Test
    public void testRepro() {
        Timestamp t1 = Timestamp.fromMicros(1739660787943000L);
        Timestamp t2 = Timestamp.fromMicros(1739660787943000L);
        client.add("start", Timestamp.fromMicros(1748750400000000L), 1);
        Set<Long> actual = client.find("start", Operator.BETWEEN, t1, t2, t1);
        Assert.assertEquals(ImmutableSet.of(1L), actual);
    }

}
