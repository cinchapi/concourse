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
package com.cinchapi.concourse.shell;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.shell.ConcourseShell;
import com.cinchapi.concourse.shell.EvaluationException;
import com.cinchapi.concourse.shell.ProgramCrash;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Strings;
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

    @Test
    public void testImportedClasssesAreAccessible() throws Throwable {
        for (Class<?> clazz : ConcourseShell.IMPORTED_CLASSES) {
            String variable = clazz.getSimpleName();
            String expected = Strings.format("Returned 'class {}'",
                    clazz.getName());
            String actual = cash.evaluate(variable);
            actual = actual.split(" in ")[0];
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testInsertListOfMaps() throws Throwable { // GH-116
        cash.evaluate("data = [['name':'John Doe','department': 'Engineering','title': 'Senior Software Engineer','role': 'Software Engineer - Backend','manager': Link.toWhere('title = Director of Engineering'),'salary': 10.00,'location': 'Atlanta','exempt': true]]");
        cash.evaluate("insert data");
        Assert.assertTrue(true);
    }

    @Test(expected = EvaluationException.class)
    public void testInsertRandomObjectFails() throws Throwable {
        cash.evaluate("insert(new Object())");
    }

    @Test
    public void testConvertCorrectMethodNamesInUnderscoreToCamelcase()
            throws IrregularEvaluationResult {
        String record = cash.evaluate("find_or_add('name', 'concourse')");
        String result = cash.evaluate("get('name', " + record.split("'")[1]
                + ")");
        Assert.assertTrue(result.contains("concourse"));
    }

    @Test(expected = EvaluationException.class)
    public void testConvertWrongMethodNamesInUnderscoreToCamelcase()
            throws IrregularEvaluationResult {
        cash.evaluate("find_or_sub('name', 'concourse')");
    }

    @Test
    public void testMultipleMethodsNameHavingMethodNameUnderscoreCase()
            throws Throwable {
        long record = TestData.getPositiveNumber().longValue();
        cash.loadExternalScript(Resources.getAbsolutePath("/sample-cashrc"));
        cash.evaluate("callA(" + record
                + "); find_or_add('name', 'concourse'); callB(" + record
                + "); add('name', 'jeff', 2);");
        String resultExt = cash.evaluate("describe " + record);
        Assert.assertTrue(resultExt.contains("[a, b]"));
    }

    @Test
    public void testInvokeNoArgMethodWithoutParensUsingFullSyntax()
            throws IrregularEvaluationResult {
        cash.evaluate("inventory");
        cash.evaluate("concourse.inventory");
        Assert.assertTrue(true); // test passes if it does not throw an
                                 // exception
    }

    @Test
    public void testBasicUnderscoreMethod() throws IrregularEvaluationResult {
        cash.evaluate("find_or_add 'foo', 1");
        Assert.assertTrue(true); // test passes if it does not throw an
                                 // exception
    }

    @Test
    public void testBasicUnderscoreMethodNoArgs()
            throws IrregularEvaluationResult {
        cash.evaluate("get_server_version");
        Assert.assertTrue(true); // test passes if it does not throw an
                                 // exception
    }
    
    @Test
    public void testKeyWithUnderscore() throws IrregularEvaluationResult {
        cash.evaluate("add 'fav_language','Go', 1");
        Map<Object, Set<Long>> map = client.browse("fav_language");
        Assert.assertTrue(map.containsKey("Go")); // test passes if it does
                                                  // not throw an
                                                  // exception
    }

   @Test(expected = EvaluationException.class)
    public void testNestedApiMethodWithoutParensDoesNotInfiniteLoop()
            throws IrregularEvaluationResult {
        //NOTE: EvaluationException is valid exit state until GH-139 is fixed.
        long record = client.add("foo", "2");
        cash.evaluate("diff \"" + record + "\", time(\"last week\")");
        Assert.assertTrue(true); // test passes if it does not throw an
                                 // exception
    }
}
