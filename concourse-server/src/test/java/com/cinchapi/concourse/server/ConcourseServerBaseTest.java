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
package com.cinchapi.concourse.server;

import org.apache.thrift.transport.TTransportException;

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

/**
 * The base class for unit tests that use {@link ConcourseServer} directly.
 * 
 * @author Jeff Nelson
 */
public class ConcourseServerBaseTest extends ConcourseBaseTest {

    /**
     * A reference to a ConcourseServer instance that can be used in each unit
     * test.
     */
    protected ConcourseServer server;

    @Override
    public void beforeEachTest() {
        try {
            server = ConcourseServer.create(Networking.getOpenPort(), Files
                    .createTempDir().getAbsolutePath(), Files.createTempDir()
                    .getAbsolutePath());
        }
        catch (TTransportException e) {
            throw Throwables.propagate(e);
        }
    }

}
