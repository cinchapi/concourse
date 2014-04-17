/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.shell;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link SyntaxTools} methods.
 * 
 * @author jnelson
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
                "time \"yesterday\"",
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
