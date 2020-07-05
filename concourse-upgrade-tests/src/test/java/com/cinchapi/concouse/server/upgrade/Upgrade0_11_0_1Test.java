/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.ClientServerTests;

/**
 * Upgrade task to ensure that data in Blocks is properly transferred to segment
 * files.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_11_0_1Test extends UpgradeTest {

    @Override
    protected String getInitialServerVersion() {
        return "0.10.4";
    }

    private String[] envs;

    @Override
    protected void preUpgradeActions() {
        envs = new String[] { "foo" };
        ClientServerTests.insertRandomData(server, envs);
    }

    @Test
    public void testTransferBlockDataToSegments() {
        //TODO: implement
        Assert.assertTrue(true);
    }

}
