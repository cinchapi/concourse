/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
 * @author Jeff Nelson
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
