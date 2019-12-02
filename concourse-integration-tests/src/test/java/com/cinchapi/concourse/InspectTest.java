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
package com.cinchapi.concourse;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;

public class InspectTest extends ConcourseIntegrationTest {

    @Test
    public void testInspect() {
        Map<String, Set<String>> describe = client.inspect();

        int enabledPlugins = describe.size();

        System.out.println(MessageFormat
                .format("There are {0} plugins enabled.", enabledPlugins));

        describe.forEach((plugin, methods) -> {
            System.out.println(MessageFormat.format("Plugin: {0} | Methods {1}",
                    plugin, methods.toArray().toString()));
        });
    }

}
