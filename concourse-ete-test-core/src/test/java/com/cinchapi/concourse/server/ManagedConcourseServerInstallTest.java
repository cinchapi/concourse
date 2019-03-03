/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the installability of various Concourse versions using the
 * {@link ManagedConcourseServer} framework.
 *
 * @author Jeff Nelson
 */
public class ManagedConcourseServerInstallTest {

    @Test
    public void testInstallVersion0_8_1() {
        testInstall(ManagedConcourseServer.manageNewServer("0.8.1"));
    }

    @Test
    public void testInstallVersionBefore0_5_0DoesNotHang() {
        testInstall(ManagedConcourseServer.manageNewServer("0.4.4"));
        // An "error" occurs in the shutdown hook that checks to see if the
        // server is still running because the Tanuki control script (used
        // before version 0.5) returns an exit code of 1 on the "status"
        // command...
        System.out.println("IGNORE EXCEPTION STACKTRACE!!!");
    }

    /**
     * Utility method to test installability of a given server.
     * 
     * @param server
     */
    private static void testInstall(ManagedConcourseServer server) {
        server.start();
        server.stop();
        Assert.assertTrue(true); // lack of exception means test passes...
    }

}
