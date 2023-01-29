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
package com.cinchapi.concourse.ete.performance;

/**
 * Utilities for cross version benchmark tests.
 *
 * @author Jeff Nelson
 */
public final class CrossVersionBenchmarkTests {

    /**
     * The versions to use in all benchmark tests
     */
    public static String[] VERSIONS = { "0.11.3", "0.11.4", "0.11.5",
            "latest" };

    private CrossVersionBenchmarkTests() {/* no-init */}

}
