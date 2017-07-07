/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.bugrepro;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;

/**
 * Repro of <a href="https://cinchapi.atlassian.net/browse/CON-72">CON-72</a>
 * where a transaction deadlock would occur when writing a key as value before
 * range reading that key with the added value.
 * 
 * @author Jeff Nelson
 */
public class CON72 extends ConcourseIntegrationTest {

    @Test
    public void repro() throws IOException {
        client.stage();
        long record = Time.now();
        client.set("__table__", "com.blavity.server.model.App", record);
        client.find("__table__", Operator.EQUALS,
                "com.blavity.server.model.App");
        Assert.assertTrue(client.commit());
    }

}
