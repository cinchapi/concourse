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
package com.cinchapi.concourse.server.plugin;

import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

public class PluginConfigurationTest {
    
    @Test
    public void testGetAliases() {
        PluginConfiguration config = new StandardPluginConfiguration(
                Paths.get(Resources.getAbsolutePath("/alias_test.plugin.prefs")));
        List<String> actual = Lists.newArrayList();
        actual.add("foo");
        actual.add("bar");
        actual.add("car");
        actual.add("dar");
        actual.add("far");
        List<String> expected = config.getAliases();
        Assert.assertEquals(expected.containsAll(actual),
                actual.containsAll(expected));
    }
    
}
