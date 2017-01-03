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
package com.cinchapi.concourse.test;

import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.test.runners.CrossVersionTestRunner;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;

/**
 * A {@link CrossVersionTest} is one that runs against multiple versions of
 * Concourse. This is beneficial for comparing performance and functionality
 * across different versions of the product in a head-to-head manner.
 * <p>
 * To simultaneously run a test case against multiple versions, make sure the
 * test class extends {@link CrossVersionTest} and uses the {@link Versions}
 * annotation to specify the release versions or paths to local installer files
 * to test against.
 * </p>
 * 
 * @author jnelson
 */
@RunWith(CrossVersionTestRunner.class)
public abstract class CrossVersionTest extends ClientServerTest {

    private String version = ""; // this is reflectively set by the test runner
                                 // for each versions that we run against

    /**
     * A {@link Logger} to print information about the test case.
     */
    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     * A collection of stats that are recorded during the test and sorted by
     * version. These stats are printed at the end of the entire test run to
     * provide an easy view of comparative metrics.
     */
    private static PrettyLinkedTableMap<String, String, Object> stats = PrettyLinkedTableMap
            .newPrettyLinkedTableMap("Version");

    @Override
    protected void beforeEachTest() {
        log.info("Running against version {}", version);
    }

    @Override
    protected final String getServerVersion() {
        return version;
    }

    /**
     * Record a stat described by the {@code key} and {@code value}. The stat
     * will be recorded for each version and printed out at the end for
     * comparison.
     * 
     * @param key
     * @param value
     */
    protected final void record(String key, Object value) {
        stats.put(version, key, value);
    }

}
