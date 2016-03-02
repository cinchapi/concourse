/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin.http;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Strings;

import static com.cinchapi.concourse.plugin.http.HttpPlugin.getCanonicalNamespace;
import static com.cinchapi.concourse.plugin.http.HttpPlugin.buildSparkPath;

/**
 * Unit tests for {@link HttpPlugin}.
 * 
 * @author Jeff Nelson
 */
public class HttpPluginTest {

    @Test
    public void testGetCanonicalNamespaceAliasFull() {
        Assert.assertEquals(
                "/",
                getCanonicalNamespace("com.cinchapi.concourse.server.http.IndexRouter"));
        Assert.assertEquals(
                "/help/",
                getCanonicalNamespace("com.cinchapi.concourse.server.http.HelpRouter"));
        Assert.assertEquals(
                "/foo-bar/",
                getCanonicalNamespace("com.cinchapi.concourse.server.http.FooBar"));
        Assert.assertEquals(
                "/foo-bar/one/",
                getCanonicalNamespace("com.cinchapi.concourse.server.http.FooBar_One"));
    }

    @Test
    public void testGetCanonicalNamespaceAliasNone() {
        Assert.assertEquals("/com/company/plugin/",
                getCanonicalNamespace("com.company.plugin.IndexRouter"));
        Assert.assertEquals("/com/company/plugin/help/",
                getCanonicalNamespace("com.company.plugin.HelpRouter"));
        Assert.assertEquals("/com/company/plugin/foo-bar/",
                getCanonicalNamespace("com.company.plugin.FooBar"));
        Assert.assertEquals("/com/company/plugin/foo-bar/one/",
                getCanonicalNamespace("com.company.plugin.FooBar_One"));
    }

    @Test
    public void testBuildSparkPath() {
        Assert.assertEquals(":key/:record/audit",
                buildSparkPath(Strings.splitCamelCase("$Key$RecordAudit")));
    }

}
