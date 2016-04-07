/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PluginId}.
 * 
 * @author Jeff Nelson
 */
public class PluginIdTest {
    
    @Test
    public void testGeneratePluginIdServer(){
        PluginId id = PluginId.forName("com.cinchapi.concourse.server.http.IndexRouter");
        Assert.assertEquals("com.cinchapi", id.group);
        Assert.assertEquals("server", id.module);
        Assert.assertEquals("IndexRouter", id.cls);
    }
    
    @Test
    public void testGeneratedPluginIdNlpModule(){
        PluginId id = PluginId.forName("com.cinchapi.nlp.http.IndexRouter");
        Assert.assertEquals("com.cinchapi", id.group);
        Assert.assertEquals("nlp", id.module);
        Assert.assertEquals("IndexRouter", id.cls);
    }

}
