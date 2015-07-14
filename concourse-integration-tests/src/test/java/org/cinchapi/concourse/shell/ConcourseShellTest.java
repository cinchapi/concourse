/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.shell;

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.util.Resources;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class ConcourseShellTest extends ConcourseIntegrationTest {

    private ConcourseShell cash = null;

    @Override
    public void beforeEachTest() {
        super.beforeEachTest();
        try {
            cash = new ConcourseShell();
            cash.concourse = this.client;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testExternalScriptUseShortSyntax() throws Throwable {
        cash.loadExternalScript(Resources
                .getAbsolutePath("/sample-cashrc-short-syntax"));
        String result = cash.evaluate("get 'name', 1");
        Assert.assertTrue(result.contains("jeff"));
    }
    
    @Test
    public void testExternalScriptUseShortSyntaxInMethod() throws Throwable {
        cash.loadExternalScript(Resources
                .getAbsolutePath("/sample-cashrc-short-syntax-in-method"));
        cash.evaluate("callMethod()");
        String result = cash.evaluate("get 'name', 1");
        Assert.assertTrue(result.contains("jeff"));
    }
    
    @Test
    public void testCallExternalScriptMethodsWithoutPrependingExt() throws Throwable {
        long record = TestData.getPositiveNumber().longValue();
        cash.loadExternalScript(Resources
                .getAbsolutePath("/sample-cashrc"));
        cash.evaluate("callA("+record+"); callB("+record+")");
        String result = cash.evaluate("describe "+record);
        Assert.assertTrue(result.contains("[a, b]"));
    }

}
