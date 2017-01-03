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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.shell.ConcourseShell;
import com.cinchapi.concourse.shell.SyntaxTools;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link SyntaxTools} methods.
 * 
 * @author Jeff Nelson
 */
public class SyntaxToolsTest {

    @Test
    public void testParseSingleInvokedMethod() {
        Set<String> methods = SyntaxTools.parseShortInvokedMethods("foo()");
        Assert.assertEquals(1, methods.size());
        Assert.assertTrue(methods.contains("foo"));
    }

    @Test
    public void testParseSingleInvokedConcourseMethod() {
        Set<String> methods = SyntaxTools
                .parseShortInvokedMethods("concourse.foo()");
        Assert.assertEquals(0, methods.size());
        Assert.assertFalse(methods.contains("concourse.foo"));
    }

    @Test
    public void testParseNestedInvokvedMethods() {
        Set<String> methods = SyntaxTools
                .parseShortInvokedMethods("foo(bar(x))");
        Assert.assertEquals(methods, Sets.newHashSet("foo", "bar"));
    }

    @Test
    public void testParseSingleInvokedBlacklistedMethod() {
        Set<String> methods = SyntaxTools.parseShortInvokedMethods("time(x)");
        Assert.assertEquals(0, methods.size());
        Assert.assertFalse(methods.contains("time"));
    }

    @Test
    public void testParseNestedInvokedBlacklistedMethod() {
        Set<String> methods = SyntaxTools
                .parseShortInvokedMethods("foo(time(x, y))");
        Assert.assertEquals(methods, Sets.newHashSet("foo"));
    }

    @Test
    public void testParseNestedInvokedConcourseMethod() {
        Set<String> methods = SyntaxTools
                .parseShortInvokedMethods("foo(concourse.bar(x, y))");
        Assert.assertEquals(methods, Sets.newHashSet("foo"));
    }

    @Test
    public void testParseNestedNestedInvokvedMethods() {
        Set<String> methods = SyntaxTools
                .parseShortInvokedMethods("foo(bar(x, baz(time(z))))");
        Assert.assertEquals(methods, Sets.newHashSet("foo", "bar", "baz"));
    }

    @Test
    public void testHandleShortSyntaxTimeWithNoArg() {
        Assert.assertEquals("time \"now\"", SyntaxTools.handleShortSyntax(
                "time",
                Lists.newArrayList(ConcourseShell.getAccessibleApiMethods())));
    }

    @Test
    public void testHandleShortSyntaxDateWithNoArg() {
        Assert.assertEquals("date \"now\"", SyntaxTools.handleShortSyntax(
                "date",
                Lists.newArrayList(ConcourseShell.getAccessibleApiMethods())));

    }

    @Test
    public void testHandleShortSyntaxTimeWithArg() {
        Assert.assertEquals(
                "concourse.time \"yesterday\"",
                SyntaxTools.handleShortSyntax("time \"yesterday\"", Lists
                        .newArrayList(ConcourseShell.getAccessibleApiMethods())));
    }

    @Test
    public void testHandleShortSyntaxDateWithArg() {
        Assert.assertEquals(
                "date \"yesterday\"",
                SyntaxTools.handleShortSyntax("date \"yesterday\"", Lists
                        .newArrayList(ConcourseShell.getAccessibleApiMethods())));
    }

    @Test
    public void testHandleShortSyntaxSingleMethodNoParens() {
        String line = "add \"foo\", \"bar\", 1";
        String expected = "concourse.add \"foo\", \"bar\", 1";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    @Test
    public void testHandleShortSyntaxSingleMethodWithParens() {
        String line = "add(\"foo\", \"bar\", 1)";
        String expected = "concourse.add(\"foo\", \"bar\", 1)";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    @Test
    public void testHandleShortSyntaxNestedMethods() {
        String line = "get(describe(1), 1)";
        String expected = "concourse.get(concourse.describe(1), 1)";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    @Test
    public void testHandleShortSyntaxNestedMethodsSomeBlacklisted() {
        String line = "get(describe(1), 1, time(\"yesterday\"))";
        String expected = "concourse.get(concourse.describe(1), 1, time(\"yesterday\"))";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    @Test
    public void testHandleShortSyntaxNestedNestedMethods() {
        String line = "get(describe(1), find(\"name\", eq, \"foo\"))";
        String expected = "concourse.get(concourse.describe(1), concourse.find(\"name\", eq, \"foo\"))";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    @Test
    public void testHandleShortSyntaxMethodWithNoArgs() {
        String line = "getServerVersion";
        String expected = "concourse.getServerVersion()";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    private static List<String> METHODS = Lists.newArrayList("add", "foo",
            "sub");

    @Test
    public void testHandleMissingArgsCommasSingleQuotes() {
        String line = "add 'name' 'jeff nelson' 1";
        String expected = "add 'name', 'jeff nelson', 1";
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void testHandleMissingArgsCommas() {
        String line = "add \"name\" \"jeff nelson\" '1 2'";
        String expected = "add \"name\", \"jeff nelson\", '1 2'";
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void testHandleMissingArgsCommasSingleArg() {
        String line = "foo 1";
        String expected = line;
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void testHandleMissingArgsCommasArgsInParenthesis() {
        String line = "add(\"name\" \"jeff nelson\" 1)";
        String expected = "add(\"name\", \"jeff nelson\", 1)";
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void testHandleMissingArgsCommasNestedMethods() {
        String line = "foo(add(1 'hello world' 3) sub(1 2 \"jeff nelson\"))";
        String expected = "foo(add(1, 'hello world', 3), sub(1, 2, \"jeff nelson\"))";
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void testHandleMissingArgsCommasOnlySomeMissing() {
        String line = "add \"name\", \"jeff\" 1";
        String expected = "add \"name\", \"jeff\", 1";
        Assert.assertEquals(expected,
                SyntaxTools.handleMissingArgCommas(line, METHODS));
    }

    @Test
    public void reproGH_127() {
        String line = "find_or_add('name', 'concourse'); callA(record); find_or_add('name', 'concourse'); callB(record); add('name', 'jeff', 2);";
        String expected = "find_or_add('name', 'concourse'); callA(record); find_or_add('name', 'concourse'); callB(record); concourse.add('name', 'jeff', 2);";
        Assert.assertEquals(expected,
                SyntaxTools.handleShortSyntax(line, options()));
    }

    /**
     * A convenience method to return the accessible API methods in CaSH as a
     * list instead of an array
     * 
     * @return the list of accessible API methods
     */
    private List<String> options() {
        return Lists.newArrayList(ConcourseShell.getAccessibleApiMethods());
    }

}
