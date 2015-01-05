/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Sets;

/**
 * A collection of tools for dealing with syntax in the {@link ConcourseShell}.
 * 
 * @author jnelson
 */
public final class SyntaxTools {

    /**
     * Check to see if {@code line} is a command that uses short syntax. Short
     * syntax allows the user to call an API method without starting the command
     * with {@code concourse.}. This method compares the line to the list of
     * {@code options} to see if it should be "expanded" from short syntax.
     * Otherwise, the original line is returned.
     * 
     * @param line
     * @param options
     * @return the expanded line, if it is using short syntax, otherwise the
     *         original line
     */
    public static String handleShortSyntax(String line, List<String> options) {
        final String prepend = "concourse.";
        if(line.equalsIgnoreCase("time") || line.equalsIgnoreCase("date")) {
            return line + " \"now\"";
        }
        else if(!line.contains("(")) {
            // If there are no parens in the line, then we assume that this is a
            // single(e.g non-nested) function invocation.
            String expanded = prepend + line;
            for (String option : options) {
                if(expanded.startsWith(option)) {
                    return expanded;
                }
            }
        }
        else {
            Set<String> shortInvokedMethods = parseShortInvokedMethods(line);
            for (String method : shortInvokedMethods) {
                if(options.contains(prepend + method)) {
                    line = line.replaceAll(method + "\\(", prepend + method
                            + "\\(");
                }
            }
        }
        return line;
    }

    /**
     * Examine a line and parse out the names of all the methods that are being
     * invoked using short syntax.
     * <p>
     * e.g. methodA(methodB(x), time(y), methodC(concourse.methodD())) -->
     * methodA, methodB, methodC
     * </p>
     * 
     * @param line
     * @return the set of all the methods which are being invoked using short
     *         syntax
     */
    protected static Set<String> parseShortInvokedMethods(String line) { // visible
                                                                         // for
                                                                         // testing
        Set<String> methods = Sets.newHashSet();
        Set<String> blacklist = Sets.newHashSet("time", "date");
        String regex = "\\b(?!" + StringUtils.join(blacklist, "|")
                + ")[\\w\\.]+\\("; // match any word followed by an paren except
                                   // for the blacklist
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
            if(!matcher.group().startsWith("concourse.")) {
                methods.add(matcher.group().replace("(", ""));
            }
        }
        return methods;
    }

    private SyntaxTools() {/* noop */}

}
