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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.server.ManagedConcourseServer;

/**
 * Utility functions for {@link ClientServerTest} and descendant classes.
 *
 * @author Jeff Nelson
 */
public final class ClientServerTests {

    private ClientServerTests() {/* no-op */}

    /**
     * A utility method to populate the test server with random data.
     * 
     * @param server the {@link ManagedConcourseServer} used in the test
     * @param environments the environments in which random data should be
     *            inserted
     */
    public static void insertRandomData(ManagedConcourseServer server,
            String... environments) {
        for (String environment : environments) {
            Path directory = server.getDatabaseDirectory().resolve(environment)
                    .resolve("cpb");
            Concourse client = server.connect("admin", "admin", environment);
            try {
                int count = Random.getScaleCount();
                Stream<Path> stream = null;
                try {
                    while (stream == null || stream.count() < count) {
                        client.add(Random.getSimpleString(),
                                Random.getObject());
                        if(stream != null) {
                            stream.close();
                        }
                        stream = java.nio.file.Files.list(directory);
                    }
                }
                finally {
                    stream.close();
                }
            }
            catch (IOException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

}
