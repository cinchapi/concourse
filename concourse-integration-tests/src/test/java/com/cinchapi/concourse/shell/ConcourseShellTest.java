/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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
package com.cinchapi.concourse.shell;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.shell.ConcourseShell;
import com.cinchapi.concourse.shell.EvaluationException;
import com.cinchapi.concourse.shell.ProgramCrash;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Throwables;

/**
 * Unit tests for CaSH functionality
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
    public void testCallExternalScriptMethodsWithoutPrependingExt()
            throws Throwable {
        long record = TestData.getPositiveNumber().longValue();
        cash.loadExternalScript(Resources.getAbsolutePath("/sample-cashrc"));
        cash.evaluate("callA(" + record + "); callB(" + record + ")");
        String result = cash.evaluate("describe " + record);
        Assert.assertTrue(result.contains("[a, b]"));
    }

    @Test(expected = EvaluationException.class)
    public void testCannotGetDeclaredFields() throws Throwable {
        cash.evaluate("concourse.getClass().getDeclaredFields()");
    }

    @Test(expected = ProgramCrash.class)
    public void testSecurityChangeCausesCrash() throws Throwable {
        grantAccess("admin", "admin2");
        cash.evaluate("add \"name\", \"jeff\", 1");
    }

}
